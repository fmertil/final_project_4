from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime

class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format

    def execute(self, context):
        self.log.info('StageToRedshiftOperator executing')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = S3Hook(aws_conn_id=self.aws_credentials_id)

        execution_date = context["execution_date"]
        timestamped_key = self.s3_key.format(execution_date=execution_date)

        s3_path = f"s3://{self.s3_bucket}/{timestamped_key}"

        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{aws_hook.get_credentials().access_key}'
            SECRET_ACCESS_KEY '{aws_hook.get_credentials().secret_key}'
            JSON '{self.json_format}'
        """

        self.log.info(f"Copying data from {s3_path} to {self.table} in Redshift")
        redshift_hook.run(copy_sql)

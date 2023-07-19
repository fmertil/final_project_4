from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        self.log.info('StageToRedshiftOperator executing')
        s3_hook = S3Hook(aws_conn_id=self.aws_credentials_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Get the S3 file path
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        
        # Create the target table in Redshift if it doesn't exist
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table}
            (
                -- Define the column schema here
                -- Example: column_name data_type
            );
        """
        redshift_hook.run(create_table_query)

        # Copy data from S3 to Redshift
        copy_query = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{s3_hook.get_credentials().access_key}'
            SECRET_ACCESS_KEY '{s3_hook.get_credentials().secret_key}'
            FORMAT AS JSON 'auto';
        """
        redshift_hook.run(copy_query)

        self.log.info(f"Data copied from {s3_path} to {self.table} in Redshift")

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DimensionLoadOperator(BaseOperator):

    ui_color = '#b4e0ff'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 truncate_before_load=True,
                 sql_query="",
                 *args, **kwargs):

        super(DimensionLoadOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.truncate_before_load = truncate_before_load
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info('DimensionLoadOperator executing')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_before_load:
            truncate_sql = f"TRUNCATE TABLE {self.target_table}"
            redshift_hook.run(truncate_sql)
            self.log.info(f"Truncated table: {self.target_table}")

        self.log.info(f"Running SQL query against {self.target_table}")
        redshift_hook.run(self.sql_query)

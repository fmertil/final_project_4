from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        self.log.info('DataQualityOperator executing')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test in self.tests:
            sql_statement = test['sql']
            expected_result = test['expected_result']

            self.log.info(f"Running data quality check: {sql_statement}")
            records = redshift_hook.get_records(sql_statement)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {sql_statement} returned no results.")

            result = records[0][0]
            if result != expected_result:
                raise ValueError(f"Data quality check failed. {sql_statement} returned {result}, expected {expected_result}.")

            self.log.info(f"Data quality check passed. {sql_statement} returned {result}, as expected.")
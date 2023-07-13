from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#ffd700'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests=[],
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

            records = redshift_hook.get_records(sql_statement)
            test_result = records[0][0]

            if test_result != expected_result:
                error_message = f"Data quality check failed. Test: {sql_statement}. Expected: {expected_result}, Got: {test_result}"
                raise ValueError(error_message)

            self.log.info(f"Data quality check passed. Test: {sql_statement}. Expected: {expected_result}, Got: {test_result}")

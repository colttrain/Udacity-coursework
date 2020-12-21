from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = 'redshift',
                 table_list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list

    def execute(self, context):
        
        #Connect to Redshift cluster
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        #Data quality check - is there data in table?
        for table in self.table_list:
            records = redshift_hook.get_records(f'SELECT COUNT(*) FROM {table}')
            num_records = records[0][0]
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Data quality check failed for {table}')
            else:
                self.log.info(f'Data quality check passed with {num_records} recorded for {table}')
            
        
        self.log.info('>>>>>>Data quality check completed<<<<<<<')
                                              
                                               
            
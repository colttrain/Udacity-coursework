from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = 'redshift',
                 table='',
                 sql_stmt='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt

    def execute(self, context):
        #Connect to redshift cluster
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info('Connection successfull')
        
        #DELETE all existing rows from table
        redshift_hook.run(f"DELETE FROM {self.table}")
      
        #Run sql to combine staging_events & staging_songs tables
        redshift_hook.run(self.sql_stmt)
        self.log.info('Insert statement has run successfully')
        
        
        
        
        

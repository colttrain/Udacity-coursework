from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id='redshift',
                 aws_credentials_id='aws_credentials',
                 s3_bucket='udacity-dend',
                 s3_key='',
                 table='',
                 filepath='',
                 timeformat='',
                 region='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table=table
        self.filepath=filepath
        self.timeformat=timeformat
        self.region=region
        
        
    def execute(self, context):
        self.log.info("<<<<<<Starting Execution>>>>>")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)

        #DELETE all existing rows from table
        redshift_hook.run(f"DELETE FROM {self.table}")
      
        key = self.s3_key.format(**context)
        
        #Get s3 filepath for sql_stmt
        s3_path = f's3://{self.s3_bucket}/{key}'
        self.log.info(s3_path)
        
        #COPY SQL stmt from s3 to redshift
        sql_stmt = f"""
                   COPY {self.table}
                   FROM '{s3_path}'
                   ACCESS_KEY_ID '{credentials.access_key}'
                   SECRET_ACCESS_KEY '{credentials.secret_key}'
                   FORMAT AS JSON '{self.filepath}'
                   TIMEFORMAT AS '{self.timeformat}'
                   region '{self.region}';
                    """
        self.log.info('SQL statement:' + sql_stmt)
        
        #Run sql_stmt to copy staging event to redshift
        redshift_hook.run(sql_stmt)
        self.log.info('SQL statement ran and staging tables created in redshift')



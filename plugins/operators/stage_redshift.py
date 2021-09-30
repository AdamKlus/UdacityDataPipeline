from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    {} '{}' 
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_credentials="",
                 s3_bucket="",
                 s3_key="",
                 file_format="JSON",
                 format_path="auto",
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.format_path = format_path
        
    def execute(self, context):
        """
        Copy files from s3 to tables in postgres
        """           
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        execution_date = context['execution_date']
        
        self.log.info(f"Truncating {self.table}")
        redshift.run(f"TRUNCATE TABLE {self.table}")
        
        
        self.log.info(f"Inserting data into {self.table}")
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        if self.s3_key == "log_data":
            year = execution_date.year
            month = execution_date.month
     
            s3_path = '/'.join([s3_path, str(year), str(month)])
            
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.file_format,
            self.format_path
        )
        
        redshift.run(formatted_sql)    




from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 append=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.append = append

    def execute(self, context):
        """
        Load dim table but check if truncate first
        """           
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # check if we need to truncate the table
        if self.append:
            self.log.info(f"Truncating table {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")
        
        self.log.info(f"Inserting data into {self.table}")
        redshift.run(str(self.sql_query))


from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import LoadDimensionOperator
from helpers import SqlQueries

def load_dim_table_dag (
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        table,
        sql_query,
        *args, **kwargs):
    """
    Subdag for dim tables - bit silly to use as its just one task
    """
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    
    load_dim_table = LoadDimensionOperator(
        task_id=f"load_{table}_table",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        sql_query=sql_query
    )
    
    load_dim_table
    
    return dag

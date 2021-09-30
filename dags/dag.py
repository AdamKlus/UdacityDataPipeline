import datetime
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, PostgresOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from subdag import load_dim_table_dag

start_date = datetime.datetime(2018, 11, 1)
end_date = datetime.datetime(2018, 11, 30)

# default arguments
default_args = {
    'owner': 'Adam Klus',
    'start_date': start_date,
    'end_date': end_date,
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# dag specs
dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# create tables
create_tables = PostgresOperator(
  task_id="Create_tables",
  dag=dag,
  sql='create_tables.sql',
  postgres_conn_id="redshift"
)

# insert events data into staging table
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    file_format="JSON",
    format_path="s3://udacity-dend/log_json_path.json"
)

# insert songs data into staging table
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    file_format="JSON",
    format_path="auto",
)

# insert songplay data into table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.songplay_table_insert
)

# insert user data into table
load_user_dimension_table = SubDagOperator(
    subdag=load_dim_table_dag(
        parent_dag_name='sparkify_dag',
        task_id='Load_users_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="users",
        sql_query=SqlQueries.user_table_insert,
        start_date=start_date,
    ),
    task_id='Load_users_table',
    dag=dag,
)

# insert songs data into table
load_song_dimension_table = SubDagOperator(
    subdag=load_dim_table_dag(
        parent_dag_name='sparkify_dag',
        task_id='Load_songs_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="songs",
        sql_query=SqlQueries.song_table_insert,
        start_date=start_date,
    ),
    task_id='Load_songs_table',
    dag=dag,
)

# insert artist data into table
load_artist_dimension_table = SubDagOperator(
      subdag=load_dim_table_dag(
        parent_dag_name='sparkify_dag',
        task_id='Load_artists_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="artists",
        sql_query=SqlQueries.artist_table_insert,
        start_date=start_date,
    ),
    task_id='Load_artists_table',
    dag=dag,
)

# insert time data into table
load_time_dimension_table = SubDagOperator(
    subdag=load_dim_table_dag(
        parent_dag_name='sparkify_dag',
        task_id='Load_time_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="time",
        sql_query=SqlQueries.time_table_insert,
        start_date=start_date,
    ),
    task_id='Load_time_table',
    dag=dag,
)

# run quality checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    tables=["songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# tasks dependancies
start_operator >> create_tables >> [stage_songs_to_redshift, stage_events_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]

[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator



from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import (LoadFactOperator, StageToRedshiftOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup_by_default': False,
    'catchup': False
}

dag = DAG('udac_example_dag',
          catchup=False,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

# start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

start_operator = PostgresOperator(
        task_id='Begin_execution',
        sql='create_tables.sql',
        postgres_conn_id='redshift',
        dag=dag
    )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table='staging_events',
    redshift_conn_id = 'redshift',
    s3_bucket = 's3://udacity-dend/log_data',
    aws_credentials_id = 'aws_credentials',
    copy_options = "'s3://udacity-dend/log_json_path.json'",
    dag=dag
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table='staging_songs',
    redshift_conn_id = 'redshift',
    s3_bucket = 's3://udacity-dend/song_data',
    aws_credentials_id = 'aws_credentials',
    copy_options = "'auto'",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    destination_table="songplays",
    songplay_table_insert=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    destination_table="users",
    sql_query=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    destination_table="songs",
    sql_query=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    destination_table="artists",
    sql_query=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    destination_table="time",
    sql_query=SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    target_table="time",
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_time_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
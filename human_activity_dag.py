import airflowlib.emr_lib as emr
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import sql_queries
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 9),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('human_activity', concurrency=1, schedule_interval=None, default_args=default_args)
region = emr.get_region()
emr.client(region_name=region)

# Creates an EMR cluster
def create_emr(**kwargs):
    cluster_id = emr.create_cluster(region_name=region, cluster_name='udacity_cluster', num_core_nodes=2)
    return cluster_id

# Waits for the EMR cluster to be ready to accept jobs
def wait_for_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.wait_for_cluster_creation(cluster_id)

# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_cluster(cluster_id)

# Converts each of the movielens datafile to parquet
def transform_humanactivity_to_parquet(**kwargs):
    # ti is the Task Instance
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url,
                                              '/root/airflow/dags/transform/humanactivity.py')
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)

def load_data_to_redshift(*args, **kwargs):
    tables = sql_queries.tables
    source_path = "s3://s3emrdendudacity/humanactivity/csv/"
    iam = "arn:aws:iam::811800901732:role/myRedshiftRole"
    redshift_hook = PostgresHook("redshift")
    for table in tables:
        src = source_path + table + '.csv'
        redshift_hook.run("""COPY public.{}
            FROM '{}'
            IAM_ROLE '{}'
            CSV;
            """.format(table, src, iam))

# Define the individual tasks using Python Operators
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

transform_humanactivity = PythonOperator(
    task_id='transform_humanactivity',
    python_callable=transform_humanactivity_to_parquet,
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

create_people_table = PostgresOperator(
    task_id="create_people_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_queries.create_tables['people']
)
create_sources_table = PostgresOperator(
    task_id="create_sources_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_queries.create_tables['sources']
)
create_collectibles_table = PostgresOperator(
    task_id="create_collectibles_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_queries.create_tables['collectibles']
)
create_time_table = PostgresOperator(
    task_id="create_time_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_queries.create_tables['time']
)
create_activities_table = PostgresOperator(
    task_id="create_activities_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_queries.create_tables['activities']
)

load_tables_from_s3 = PythonOperator(
    task_id='load_tables',
    python_callable=load_data_to_redshift,
    dag=dag
)


# construct the DAG by setting the dependencies
create_cluster >> wait_for_cluster_completion >> transform_humanactivity >> terminate_cluster
terminate_cluster >> create_people_table >> load_tables_from_s3
terminate_cluster >> create_sources_table >> load_tables_from_s3
terminate_cluster >> create_collectibles_table >> load_tables_from_s3
terminate_cluster >> create_time_table >> load_tables_from_s3
terminate_cluster >> create_activities_table >> load_tables_from_s3
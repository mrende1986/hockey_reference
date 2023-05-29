from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from yearly import run_hockey_ref_to_s3, s3_to_postgres

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'hockey_etl_pipeline_dag',
    default_args=default_args,
    description='Pipeline to extract game logs from hockey-reference.com',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='complete_run_hockey_ref_to_s3',
    python_callable=run_hockey_ref_to_s3,
    dag=dag, 
)

load_to_postgres = PythonOperator(
    task_id='complete_s3_to_postgres',
    python_callable=s3_to_postgres,
    dag=dag, 
)


run_etl >> load_to_postgres

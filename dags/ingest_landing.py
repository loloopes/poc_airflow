from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta, datetime
from include.ingest_landing.ingest_excel_landing_operator import _store_excel_file
from include.ingest_landing.ingest_mssql_landing_operator import _store_mssql_data

default_args = {
    'owner': 'loloopes',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1)
}

@dag(
    dag_id="ingest_landing",
    start_date=datetime(2025, 4, 3),
    schedule_interval='@daily',
    catchup=False,
    tags=['save_from_excel', 'saves_from_mssql'],
    description='Loads all CSVs, data from mssql to S3 landing for further transformation',
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=6),
    max_consecutive_failed_dag_runs=2,
)
def ingest_landing():

    @task()
    def start_dag():
        print(f"[I.N.F.O] Start dag!!!!!!!!!!!!!!!!!1")

    @task()
    def ingest_raw_excel():
        _store_excel_file("/usr/local/airflow/temp/excel/")

    # Store mssql data here

    @task()
    def ingest_landing_is_done():
        print(f"[I.N.F.O] Dag ingest_landing ran successfully!!!")

    # Define TriggerDagRunOperator inside DAG context
    def _trigger_landing_to_bronze():
        return TriggerDagRunOperator(
            task_id="trigger_landing_to_bronze",
            trigger_dag_id="landing_to_bronze",
            wait_for_completion=False,
            reset_dag_run=True
        )

    start_dag() >> ingest_raw_excel() >> ingest_landing_is_done() >> _trigger_landing_to_bronze()

ingest_landing()


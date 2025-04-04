from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta, datetime
from include.ingest_bronze.ingest_excel_bronze_operator import _store_excel_file
from include.ingest_bronze.ingest_mssql_bronze_operator import _store_mssql_data

default_args = {
    'owner': 'loloopes',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1)
}

@dag(
    dag_id="ingest_bronze",
    start_date=datetime(2025, 4, 3),
    schedule_interval='@daily',
    catchup=False,
    tags=['save_from_excel', 'saves_from_mssql'],
    description='Loads all CSVs, data from mssql to S3 landing for further transformation',
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=6),
    max_consecutive_failed_dag_runs=2,
)
def ingest_bronze():

    @task()
    def start_dag():
        print(f"[I.N.F.O] Start dag!!!!!!!!!!!!!!!!!")

    @task()
    def ingest_raw_excel():
        _store_excel_file("/usr/local/airflow/temp/excel/")

    @task()
    def ingest_from_protheus():
        _store_mssql_data()

    @task()
    def ingest_bronze_is_done():
        print(f"[I.N.F.O] Dag ingest_bronze ran successfully!!!")

    # Define TriggerDagRunOperator inside DAG context
    # def _trigger_landing_to_bronze():
    #     return TriggerDagRunOperator(
    #         task_id="trigger_landing_to_bronze",
    #         trigger_dag_id="landing_to_bronze",
    #         wait_for_completion=False,
    #         reset_dag_run=True
    #     )

    start_dag() >> ingest_raw_excel() >> ingest_from_protheus() >> ingest_bronze_is_done()

ingest_bronze()


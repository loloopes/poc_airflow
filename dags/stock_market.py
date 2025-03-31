from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, BUCKET_NAME

import requests

default_args = {
    'owner': 'loloopes',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1)
}

SYMBOL = 'NVDA'

@dag(
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['stock_market'],
    description='A simple DAG for stock market analysis',
    default_args=default_args,
    dagrun_timeout=timedelta(20),
    max_consecutive_failed_dag_runs=2,
    on_success_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG stock_market has succeded',
        channel='airflow'
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG stock_market has failed',
        channel='airflow'
    ),
)
def stock_market():
    
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url, 'URL')
        response = requests.get(url, headers=api.extra_dejson['headers'])
        print(response, 'RESPONSE')
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL}
    )

    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'}
   )
    
    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove='success',
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids="store_prices") }}'
        }
    )

    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs= {
            'path': '{{ ti.xcom_pull(task_ids="store_prices") }}'
        }
    )

    load_to_dw = aql.load_file(
        task_id='load_to_dw',
        input_file=File(
            path=f's3://{BUCKET_NAME}/{{{{ ti.xcom_pull(task_ids="get_formatted_csv") }}}}',
            conn_id='minio'
        ),
        output_table=Table(
            name='stock_market',
            conn_id='postgres',
            metadata=Metadata(
                schema='public'
            )
        ),
        load_options={
            'aws_access_key_id': BaseHook.get_connection('minio').login,
            'aws_secret_access_key': BaseHook.get_connection('minio').password,
            'endpoint_url': BaseHook.get_connection('minio').host,
        }
    )

    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw

stock_market()
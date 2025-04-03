from airflow.decorators import dag, task
from datetime import timedelta, datetime
from airflow.providers.docker.operators.docker import DockerOperator


default_args = {
    'owner': 'loloopes',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="landing_to_bronze",
    start_date=datetime(2025, 4, 3),
    schedule_interval=None,  # Important: no auto schedule — only triggered
    catchup=False,
    default_args=default_args,
    tags=['bronze', 'landing'],
)
def landing_to_bronze():

    @task()
    def start_dag():
        print(f'[I.N.F.O] Dag landing_to_bronze_started')

    run_transform_and_load = DockerOperator(
        task_id="run_transform_and_load",
        image="airflow/run_transform_and_load",  # name of the image you build for Excel > Delta
        docker_url="tcp://docker-proxy:2375",
        network_mode="container:spark-master",
        container_name='format_excel',
        api_version="auto",
        auto_remove='success',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "ENDPOINT": "http://host.docker.internal:9000",
        },
    )

    @task()
    def landing_to_bronze_is_done():
        print(f"[I.N.F.O] Dag landing_to_bronze ran successfully!!!")

    start_dag() >> run_transform_and_load >> landing_to_bronze_is_done()

landing_to_bronze()

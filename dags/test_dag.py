from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

@dag(
    dag_id="mssql_test",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["mssql", "test"]
)
def mssql_test():
    start = EmptyOperator(task_id="start")

    run_query = SQLExecuteQueryOperator(
        task_id="run_sql_query",
        conn_id="ms_sql_server",  # Make sure this matches your connection ID
        sql="SELECT * FROM SA1010;",
    )

    print(run_query)

    end = EmptyOperator(task_id="end")

    start >> run_query >> end

mssql_test()

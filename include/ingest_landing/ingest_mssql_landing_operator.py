from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import logging


def _store_mssql_data():
    logging.info("[DEBUG]: Entered _store_mssql_data_with_debug()")

    try:
        hook = MsSqlHook(mssql_conn_id="mssql")
        logging.info("[DEBUG]: MsSqlHook created successfully.")
        logging.info(f"[DEBUG]: Hook details: {hook}")

        conn = hook.get_conn()
        logging.info(f"[DEBUG]: Connection object: {conn}")

        query = "SELECT TOP 5 * FROM SA1010"
        logging.info(f"[DEBUG]: Executing query: {query}")
        records = hook.get_records(query)

        logging.info(f"[DEBUG]: Query returned {len(records)} rows")
        for idx, row in enumerate(records, start=1):
            logging.info(f"[DEBUG]: Row {idx}: {row}")

    except Exception as e:
        logging.error(f"[ERROR]: Failed to fetch data from MSSQL: {e}", exc_info=True)
        raise
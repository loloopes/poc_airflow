from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import logging

unique_elements = [
    'SCT010', 'SC5010', 'SA1010', 'SE1010', 'SC6010', 'SD2010', 'SF2010',
    'SB1010', 'SD1010', 'Drive', 'SA3010', 'SZA010', 'SZB010', 'SZC010',
    'SM0010', 'SYS_COMPANY', 'ZMT010', 'SYS-COMPANY', 'SA2010', 'CT2010',
    'CTK010', 'CT2011', 'SF1010', 'FK1010', 'FK2011', 'SN3010', 'CT1010', 
    'AKD010', 'CTD010', 'SC7010', 'SRJ010', 'SRV010', 'SRC010', 'SRD010',
    'RG1010', 'SRG010', 'SRR010', 'SRB010', 'SRE010', 'RGB010', 'Tdsoft',
    'SRH010', 'SD2010', 'SRA010', 'CTG010', 'CTT010'
]

def _store_mssql_data():
    logging.info("[DEBUG]: Entered _store_mssql_data_with_debug()")

    try:
        hook = MsSqlHook(mssql_conn_id="mssql")
        logging.info("[DEBUG]: MsSqlHook created successfully.")
        logging.info(f"[DEBUG]: Hook details: {hook}")

        conn = hook.get_conn()
        logging.info(f"[DEBUG]: Connection object: {conn}")

        query = "SELECT TOP 5 * FROM MP12HML.dbo.RGB010"
        logging.info(f"[DEBUG]: Executing query: {query}")
        records = hook.get_records(query)

        logging.info(f"[DEBUG]: Query returned {len(records)} rows")
        for idx, row in enumerate(records, start=1):
            logging.info(f"[DEBUG]: Row {idx}: {row}")

    except Exception as e:
        logging.error(f"[ERROR]: Failed to fetch data from MSSQL: {e}", exc_info=True)
        raise
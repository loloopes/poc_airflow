import os
import logging
import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from deltalake.writer import write_deltalake

# MinIO settings (assumes bucket exists)
S3_BUCKET = "s3a://bucket-data/bronze/mssql/"
S3_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": "minio",
    "AWS_SECRET_ACCESS_KEY": "minio123",
    "AWS_ENDPOINT_URL": "http://minio:9000",
}

unique_elements = [
    'SCT010', 'SC5010', 'SA1010', 'SE1010', 'SC6010', 'SD2010', 'SF2010',
    'SB1010', 'SD1010', 'Drive', 'SA3010', 'SZA010', 'SZB010', 'SZC010',
    'SM0010', 'SYS_COMPANY', 'ZMT010', 'SYS-COMPANY', 'SA2010', 'CT2010',
    'CTK010', 'CT2011', 'SF1010', 'FK1010', 'FK2011', 'SN3010', 'CT1010', 
    'AKD010', 'CTD010', 'SC7010', 'SRJ010', 'SRV010', 'SRC010', 'SRD010',
    'RG1010', 'SRG010', 'SRR010', 'SRB010', 'SRE010', 'RGB010', 'Tdsoft',
    'SRH010', 'SRA010', 'CTG010', 'CTT010'
]

tables = ['SCT010']

def _store_mssql_data():
    print("[D.E.B.U.G.]: Starting MSSQL ingestion with pandas to Delta on MinIO")

    hook = MsSqlHook(mssql_conn_id="mssql")

    for table in tables:
        try:
            print(f"[D.E.B.U.G.]: Querying table: {table}")
            query = f"SELECT * FROM MP12HML.dbo.{table}"
            df = hook.get_pandas_df(query)
            row_count = len(df)

            print(f'[I.N.F.O]: Table {table}, with {row_count} rows!!!!')

            if df.empty:
                print(f"[W.A.R.N.]: Table {table} returned no data")
                continue

            delta_path = f"{S3_BUCKET}{table.lower()}"
            print(f"[D.E.B.U.G.]: Writing to Delta: {delta_path}")
            
            write_deltalake(
                delta_path,
                data=df,
                mode="overwrite",  # Ou "append"
                storage_options=S3_STORAGE_OPTIONS
            )


            print(f"[I.N.F.O.]: Successfully wrote {len(df)} rows to {delta_path}")
        except Exception as e:
            print(f"[E.R.R.O.R.]: Failed on table {table}: {e}")

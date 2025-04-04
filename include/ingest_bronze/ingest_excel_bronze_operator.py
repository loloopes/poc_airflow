from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO
from pathlib import Path

BUCKET_NAME = "bucket-data"

def _get_minio_client():
    conn = BaseHook.get_connection("minio")
    endpoint = conn.extra_dejson["endpoint_url"].split("//")[1]
    return Minio(
        endpoint=endpoint,
        access_key=conn.login,
        secret_key=conn.password,
        secure=False
    )

def _store_excel_file(directory):
    """
    Uploads all CSV files in the given directory to MinIO under landing/bike-shop/{file_name}.
    """
    base_path = Path(directory).resolve()
    client = _get_minio_client()

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    for csv_file in base_path.glob("*.csv"):
        with open(csv_file, "rb") as f:
            data = f.read()

        file_name = csv_file.name
        object_path = f"landing/bike-shop/{file_name}"  # <-- SINGLE DIR for all

        try:
            client.stat_object(BUCKET_NAME, object_path)
            client.remove_object(BUCKET_NAME, object_path)
            print(f"[INFO] Removed previous version of {object_path}")
        except Exception as e:
            print(f"[INFO] No previous file to remove for {object_path}: {e}")

        client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=object_path,
            data=BytesIO(data),
            length=len(data)
        )

        print(f"[SUCCESS] Uploaded {file_name} to s3://{BUCKET_NAME}/{object_path}")
    
    print("[SUCCESS] All Excel tables were loaded and overwritten into 'bike-shop' directory.")

    print("[SUCCESS] All excel table were loaded and overwrited the previous files if there were any")
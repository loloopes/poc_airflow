from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import boto3
import os
import sys

if __name__ == '__main__':

    def app():
        # Required ENV
        input_path = os.getenv("SPARK_APPLICATION_ARGS")  # e.g., landing/bike-shop/
        if not input_path:
            raise ValueError("SPARK_APPLICATION_ARGS is required. It should point to the landing path.")

        # Setup S3 info
        access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
        endpoint = os.getenv("ENDPOINT", "http://host.docker.internal:9000")

        bucket = "bucket-data"
        s3_prefix = input_path.strip("/")
        output_prefix = s3_prefix.replace("landing", "bronze")

        # Create SparkSession
        spark = SparkSession.builder.appName("FormatExcel") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,com.crealytics:spark-excel_2.12:0.13.5") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("fs.s3a.access.key", access_key) \
            .config("fs.s3a.secret.key", secret_key) \
            .config("fs.s3a.endpoint", endpoint) \
            .config("fs.s3a.connection.ssl.enabled", "false") \
            .config("fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

        # Use boto3 to list Excel files
        s3 = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

        response = s3.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)
        files = [f["Key"] for f in response.get("Contents", []) if f["Key"].endswith(".xlsx")]

        if not files:
            print(f"[I.N.F.O] No Excel files found at s3a://{bucket}/{s3_prefix}")
            return

        for key in files:
            try:
                file_name = key.split("/")[-1].replace(".xlsx", "")
                input_file = f"s3a://{bucket}/{key}"
                output_path = f"s3a://{bucket}/{output_prefix}/{file_name}"

                print(f"[I.N.F.O] Reading: {input_file}")
                df = spark.read.format("com.crealytics.spark.excel") \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .load(input_file)

                df = df.withColumn("ingestion_timestamp", current_timestamp())

                print(f"[I.N.F.O] Writing Delta to: {output_path}")
                df.write.format("delta").mode("overwrite").save(output_path)

            except Exception as e:
                print(f"[E.R.R.O.R] Failed to process {key}: {str(e)}")

        spark.stop()

    app()
    os.system('kill %d' % os.getpid())

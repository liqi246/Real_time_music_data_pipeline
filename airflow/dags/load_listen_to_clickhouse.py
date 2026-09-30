import os
import json
import tempfile
from datetime import datetime, timedelta

import oss2
import pyarrow.parquet as pq
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context


def load_hourly_listen_events():

    # Get the current Airflow task context
    context = get_current_context()

    # Airflow logical execution time
    logical_date = context["logical_date"]

    # Process data from the previous hour
    target_time = logical_date - timedelta(hours=1)

    month = target_time.month
    day = target_time.day
    hour = target_time.hour

    print(
        f"Processing partition: "
        f"month={month}, day={day}, hour={hour}"
    )

    
    # Build the OSS partition path
    #
    # Example:
    # streamify/listen_events/month=9/day=20/hour=10/

    prefix = (
        f"streamify/listen_events/"
        f"month={month}/"
        f"day={day}/"
        f"hour={hour}/"
    )

    print(f"OSS prefix: {prefix}")

    
    # Connect to Alibaba Cloud OSS
    
    auth = oss2.Auth(
        os.environ["OSS_ACCESS_KEY_ID"],
        os.environ["OSS_ACCESS_KEY_SECRET"]
    )

    bucket = oss2.Bucket(
        auth,
        os.environ["OSS_ENDPOINT"],
        os.environ["OSS_BUCKET"]
    )

    
    # Find all Parquet files in the target partition
    parquet_files = []

    for obj in oss2.ObjectIterator(bucket, prefix=prefix):
        if obj.key.endswith(".parquet"):
            parquet_files.append(obj.key)

    print(f"Found {len(parquet_files)} Parquet file(s)")

    # No data for this hour: skip instead of failing the DAG
    if not parquet_files:
        print(f"No data found for partition: {prefix}")
        return


    # Read ClickHouse connection settings
    clickhouse_host = os.getenv(
        "CLICKHOUSE_HOST",
        "host.docker.internal"
    )

    clickhouse_port = os.getenv(
        "CLICKHOUSE_PORT",
        "8123"
    )

    clickhouse_user = os.environ["CLICKHOUSE_USER"]
    clickhouse_password = os.environ["CLICKHOUSE_PASSWORD"]

    clickhouse_url = (
        f"http://{clickhouse_host}:{clickhouse_port}/"
    )


    # Delete existing data for the target hour
    # This makes the hourly load idempotent:
    # retrying the same hour will not create duplicate rows.
    delete_sql = f"""
    ALTER TABLE streamify.listen_events
    DELETE WHERE
        month = {month}
        AND day = {day}
        AND hour = {hour}
    """

    delete_response = requests.post(
        clickhouse_url,
        params={
            "user": clickhouse_user,
            "password": clickhouse_password,
            "query": delete_sql
        },
        timeout=30
    )

    delete_response.raise_for_status()

    print("Existing ClickHouse partition data deleted")

    # Process every Parquet file in the hourly partition
    total_rows = 0

    for parquet_key in parquet_files:

        print(f"Processing file: {parquet_key}")

        # Create a temporary local Parquet file
        with tempfile.NamedTemporaryFile(
            suffix=".parquet"
        ) as tmp_file:

            # Download the Parquet object from OSS
            bucket.get_object_to_file(
                parquet_key,
                tmp_file.name
            )

            # Read the Parquet file with PyArrow
            table = pq.read_table(tmp_file.name)

            rows = table.to_pylist()

            if not rows:
                print("Parquet file is empty, skipping")
                continue

            cleaned_rows = []

            # Transform fields before inserting
            for row in rows:

                # Convert Python datetime to a ClickHouse-friendly string
                if row["ts"] is not None:
                    row["ts"] = row["ts"].strftime(
                        "%Y-%m-%d %H:%M:%S.%f"
                    )

                # Add partition columns that are stored in the OSS path
                row["month"] = month
                row["day"] = day
                row["hour"] = hour

                cleaned_rows.append(row)

            # Convert records to ClickHouse JSONEachRow format
            data = "\n".join(
                json.dumps(
                    row,
                    ensure_ascii=False
                )
                for row in cleaned_rows
            )

            
            #  Batch insert into ClickHouse
            
            insert_sql = """
            INSERT INTO streamify.listen_events
            FORMAT JSONEachRow
            """

            insert_response = requests.post(
                clickhouse_url,
                params={
                    "user": clickhouse_user,
                    "password": clickhouse_password,
                    "query": insert_sql
                },
                data=data.encode("utf-8"),
                timeout=60
            )

            insert_response.raise_for_status()

            total_rows += len(cleaned_rows)

            print(
                f"Inserted {len(cleaned_rows)} rows "
                f"from {parquet_key}"
            )

    print(
        f"Hourly load completed. "
        f"Total inserted rows: {total_rows}"
    )


# Airflow DAG definition
with DAG(
    dag_id="load_listen_to_clickhouse_hourly",

    # The DAG becomes eligible for scheduling from this date
    start_date=datetime(2026, 9, 20),

    # Run automatically at minute 5 of every hour
    # Examples: 10:05, 11:05, 12:05
    schedule="5 * * * *",

    # Do not automatically run all historical intervals
    catchup=False,

    tags=["streamify", "listen", "hourly"]
) as dag:

    load_task = PythonOperator(
        task_id="load_listen_events",
        python_callable=load_hourly_listen_events
    )

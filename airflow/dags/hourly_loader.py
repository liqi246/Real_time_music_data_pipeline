
import os
import json
import tempfile
from datetime import timedelta

import oss2
import pyarrow.parquet as pq
import requests

from airflow.operators.python import get_current_context


def load_hourly_event(event_name):
    """
    Load the previous completed hourly partition from OSS into ClickHouse.

    Parameters
    ----------
    event_name : str
        Event type such as listen_events, auth_events,
        or page_view_events.
    """

    # Get the current Airflow task context
    context = get_current_context()

    # Process the previous completed hour
    target_time = context["data_interval_end"] - timedelta(hours=1)

    year = target_time.year
    month = target_time.month
    day = target_time.day
    hour = target_time.hour

    print(
        f"Processing {event_name}: "
        f"year={year}, month={month}, day={day}, hour={hour}"
    )

    # Build the OSS hourly partition path
    prefix = (
        f"streamify/{event_name}/"
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

    # Find every Parquet file in the target hour
    parquet_files = []

    for obj in oss2.ObjectIterator(bucket, prefix=prefix):
        if obj.key.endswith(".parquet"):
            parquet_files.append(obj.key)

    print(f"Found {len(parquet_files)} Parquet file(s)")

    # An empty hour is not treated as a pipeline failure
    if not parquet_files:
        print(f"No data found for {prefix}")
        return

    # Read ClickHouse connection information from environment variables
    clickhouse_host = os.environ["CLICKHOUSE_HOST"]
    clickhouse_port = os.getenv("CLICKHOUSE_PORT", "8123")
    clickhouse_user = os.environ["CLICKHOUSE_USER"]
    clickhouse_password = os.environ["CLICKHOUSE_PASSWORD"]

    clickhouse_url = f"http://{clickhouse_host}:{clickhouse_port}/"

    total_rows = 0
    staged_files = []

    # Download and validate all files before deleting existing data
    with tempfile.TemporaryDirectory() as temp_dir:

        for index, parquet_key in enumerate(parquet_files):
            print(f"Processing file: {parquet_key}")

            parquet_path = os.path.join(
                temp_dir,
                f"input_{index}.parquet"
            )

            # Download the Parquet object from OSS
            bucket.get_object_to_file(
                parquet_key,
                parquet_path
            )

            # Read Parquet data with PyArrow
            table = pq.read_table(parquet_path)
            rows = table.to_pylist()

            cleaned_rows = []

            for row in rows:
                # The OSS path does not include year
                if row.get("year") != year:
                    continue

                # Convert timestamp into a ClickHouse-friendly format
                if row.get("ts") is not None:
                    row["ts"] = row["ts"].strftime(
                        "%Y-%m-%d %H:%M:%S.%f"
                    )

                # Restore partition fields from the OSS path
                row["month"] = month
                row["day"] = day
                row["hour"] = hour

                cleaned_rows.append(row)

            if not cleaned_rows:
                print(f"No matching rows in {parquet_key}")
                continue

            # Stage the prepared records locally
            staged_path = os.path.join(
                temp_dir,
                f"ready_{index}.jsonl"
            )

            with open(
                staged_path,
                "w",
                encoding="utf-8"
            ) as staged_file:
                for row in cleaned_rows:
                    staged_file.write(
                        json.dumps(row, ensure_ascii=False) + "\n"
                    )

            staged_files.append(
                (parquet_key, staged_path, len(cleaned_rows))
            )

            total_rows += len(cleaned_rows)

        # Do not delete existing data when no matching rows are found
        if total_rows == 0:
            print(
                f"No matching data for "
                f"{year}-{month:02d}-{day:02d} {hour:02d}:00. "
                "Existing ClickHouse data was not deleted."
            )
            return

        # Delete existing rows only after all files are prepared
        delete_sql = f"""
        ALTER TABLE streamify.{event_name}
        DELETE WHERE
            year = {year}
            AND month = {month}
            AND day = {day}
            AND hour = {hour}
        SETTINGS mutations_sync = 1
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

        print("Existing hourly data deleted")

        # Insert the prepared records into ClickHouse
        inserted_rows = 0

        for parquet_key, staged_path, row_count in staged_files:
            insert_sql = f"""
            INSERT INTO streamify.{event_name}
            FORMAT JSONEachRow
            """

            with open(staged_path, "rb") as staged_file:
                insert_response = requests.post(
                    clickhouse_url,
                    params={
                        "user": clickhouse_user,
                        "password": clickhouse_password,
                        "query": insert_sql
                    },
                    data=staged_file,
                    timeout=60
                )

            insert_response.raise_for_status()

            inserted_rows += row_count

            print(
                f"Inserted {row_count} rows "
                f"from {parquet_key}"
            )

        print(
            f"{event_name} hourly load completed. "
            f"{year}-{month:02d}-{day:02d} {hour:02d}:00. "
            f"Total inserted rows: {inserted_rows}"
        )

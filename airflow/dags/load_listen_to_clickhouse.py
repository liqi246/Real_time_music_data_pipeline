from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from hourly_loader import load_hourly_event


with DAG(
    dag_id="load_listen_to_clickhouse_hourly",

    # The DAG becomes eligible for scheduling from this date
    start_date=datetime(2026, 9, 20),

    # Run automatically at minute 5 of every hour
    schedule="5 * * * *",

    # Do not automatically backfill all historical intervals
    catchup=False,

    tags=["streamify", "listen", "hourly"]
) as dag:

    load_task = PythonOperator(
        task_id="load_listen_events",

        # Reuse the common hourly loader
        python_callable=load_hourly_event,

        # Tell the loader which event table to process
        op_kwargs={
            "event_name": "listen_events"
        },
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

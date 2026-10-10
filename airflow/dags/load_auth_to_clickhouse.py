
from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from hourly_loader import load_hourly_event


with DAG(
    dag_id="load_auth_to_clickhouse_hourly",

    start_date=datetime(2026, 9, 20),

    # Run automatically at minute 5 of every hour
    schedule="5 * * * *",

    catchup=False,

    tags=["streamify", "auth", "hourly"]
) as dag:

    load_task = PythonOperator(
        task_id="load_auth_events",

        python_callable=load_hourly_event,

        op_kwargs={
            "event_name": "auth_events"
        },
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

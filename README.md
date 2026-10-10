# Real-Time Music Data Pipeline

A data engineering project that simulates music streaming events, processes them in real time, stores them in object storage, and loads them into an analytical database.

## Architecture

```mermaid
flowchart LR
    A[Eventsim] --> B[Apache Kafka]
    B --> C[Spark Structured Streaming]
    C --> D[Alibaba Cloud OSS]
    D --> E[Apache Airflow]
    E --> F[ClickHouse]
```

## Tech Stack

| Technology | Purpose |
|---|---|
| Eventsim | Generates simulated music streaming events |
| Apache Kafka | Buffers and distributes event streams |
| Spark Structured Streaming | Consumes, parses, and transforms events |
| Alibaba Cloud OSS | Stores processed data in Parquet format |
| Apache Airflow | Orchestrates scheduled data loading |
| ClickHouse | Supports analytical queries |
| Docker Compose | Runs supporting services |
| Python | Implements stream processing and orchestration logic |

## Event Types

The pipeline currently covers three event types:

- `listen_events` — music listening activity
- `auth_events` — user authentication events
- `page_view_events` — page-view activity

## Data Flow

1. Eventsim generates simulated music streaming events.
2. Kafka receives the events through separate topics.
3. Spark Structured Streaming consumes and transforms the events.
4. Spark writes processed records to Alibaba Cloud OSS in Parquet format.
5. Airflow schedules hourly loading tasks that read Parquet files from OSS.
6. The processed records are loaded into ClickHouse for analytical queries.

## Data Storage

Processed data is organized in OSS by event type and time partitions.

Example path:

```text
streamify/listen_events/
  month=9/
    day=20/
      hour=10/
        part-xxxxx.parquet
```

Each event type has its own output path and streaming checkpoint path.

## Repository Structure

```text
.
├── airflow/
│   ├── dags/
│   │   ├── hourly_loader.py
│   │   ├── load_auth_to_clickhouse.py
│   │   ├── load_listen_to_clickhouse.py
│   │   └── load_page_view_to_clickhouse.py
│   ├── Dockerfile
│   └── docker-compose.yaml
├── clickhouse/
│   ├── docker-compose.yaml
│   └── sql/
│       └── init_table.sql
├── kafka/
│   └── docker-compose.yml
├── spark_streaming/
│   ├── process_auth_events.py
│   ├── process_listen_events.py
│   ├── process_page_view_events.py
│   ├── schema.py
│   └── streaming_functions.py
├── .env.example
└── README.md
```

## Prerequisites

- Ubuntu or another Linux environment
- Python 3
- Java compatible with the installed Spark version
- Docker Engine and Docker Compose
- An Alibaba Cloud OSS bucket and credentials
- Network access between Spark, Kafka, OSS, Airflow, and ClickHouse

Versions and deployment details should be verified against the environment used to run the project.

## Configuration

Create a local environment file from the example template:

```bash
cp .env.example .env
vim .env
```

Set the required values in `.env`, including:

- Kafka host and port
- OSS endpoint, bucket name, and credentials
- ClickHouse connection settings
- Airflow secret key and administrator credentials

Use your own credentials. Never commit `.env` or real access keys to version control.

The Spark scripts read configuration from environment variables. When running Spark directly on the host, load the root environment file first:

```bash
set -a
source .env
set +a
```

`source .env` loads the variables into the current shell; it does not automatically configure separate containers unless the Compose configuration passes those variables to them.

## Running the Pipeline

Start and validate each component in stages:

1. Start Kafka and confirm that the required topics exist.
2. Start Eventsim and verify that events are reaching Kafka.
3. Load the root environment variables and start the Spark streaming jobs.
4. Verify that Parquet files are being written to the expected OSS paths.
5. Start ClickHouse and confirm that the event tables exist.
6. Start Airflow and verify that the scheduled DAGs can read OSS data and load ClickHouse.

Use the Compose files and scripts in this repository for the relevant components. Confirm service names, networking, and required environment variables before starting the complete pipeline.

## Data Validation

Useful checks include:

- Confirm that Kafka topics receive events.
- Inspect the OSS output prefixes for Parquet files.
- Check the Airflow DAG runs and task logs.
- Query ClickHouse to confirm that records are being loaded.

Example ClickHouse query:

```sql
SELECT count(*) FROM listen_events;
```

Run the query in the `streamify` database using the configured ClickHouse credentials.

## Troubleshooting

- **Kafka connection failures:** Check the advertised listener, host, port, and whether Spark runs on the host or inside a container.
- **OSS access failures:** Verify the endpoint, bucket, permissions, credentials, and system clock.
- **Spark checkpoint errors:** Ensure each streaming query uses its own checkpoint path.
- **Airflow loading failures:** Inspect task logs and verify OSS and ClickHouse connectivity.
- **Missing tables:** Confirm that the `streamify` database exists and that the initialization SQL was executed.

## Security

- Keep `.env` out of Git.
- Use placeholders in `.env.example`.
- Rotate any credential that may have been exposed.
- Grant cloud credentials only the permissions required by the pipeline.

## Project Status

The repository is being organized for reproducible deployment. Verify the Compose configurations and end-to-end pipeline in a clean environment before treating the setup as production-ready.

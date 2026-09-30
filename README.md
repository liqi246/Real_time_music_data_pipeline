# Real-Time Music Data Pipeline

A real-time data engineering project for processing simulated music streaming events.

## Architecture

Eventsim
→ Kafka
→ Spark Structured Streaming
→ Alibaba Cloud OSS
→ Airflow
→ ClickHouse

## Tch Stack

- Kafka
- Spark Structured Streaming
- Apache Airflow
- ClickHouse
- Alibaba Cloud OSS
- Docker
- Python
- Parquet

## Event Types

- listen_events
- auth_events
- page_view_events

## Pipeline

1. Eventsim generates simulated music streaming events.
2. Kafka receives events through different topics.
3. Spark Structured Streaming consumes and processes Kafka events.
4. Processed data is written to Alibaba Cloud OSS in Parquet format.
5. Airflow loads Parquet data from OSS into ClickHouse.
6. ClickHouse provides fast OLAP queries.

## OSS Partition Structure

Example:

```text
streamify/listen_events/
  month=9/
    day=20/
      hour=10/
        part-xxxxx.parquet

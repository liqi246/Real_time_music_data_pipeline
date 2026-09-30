import os

from pyspark.sql import SparkSession

from streaming_functions import (
    create_kafka_read_stream,
    process_stream
)

from schema import schema

# Create SparkSession
spark = (
    SparkSession.builder
    .appName("Process Auth Events To OSS")
    .master("local[*]")
    .getOrCreate()
)

# Reduce Log Output
spark.sparkContext.setLogLevel("WARN")


# Configure  Spark To Access The Alibaba OSS
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set(
    "fs.oss.impl",
    "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem"
)

hadoop_conf.set(
    "fs.oss.endpoint",
    os.environ["OSS_ENDPOINT"]
)

hadoop_conf.set(
    "fs.oss.accessKeyId",
    os.environ["OSS_ACCESS_KEY_ID"]
)

hadoop_conf.set(
    "fs.oss.accessKeySecret",
    os.environ["OSS_ACCESS_KEY_SECRET"]
)
# Read auth_events From Kafka
auth_events = create_kafka_read_stream(
    spark=spark,
    kafka_address="localhost",
    kafka_port="9092",
    topic="auth_events",

    # Start From The Earlist Position
    starting_offset="earliest"
)


# Parse Kafka JSON according to the schema of auth_events
auth_events = process_stream(
    auth_events,
    schema["auth_events"],
    "auth_events"
)


# Define the Output path of OSS
bucket = os.environ["OSS_BUCKET"]

output_path = (
    f"oss://{bucket}/streamify/auth_events"
)

checkpoint_path = (
    f"oss://{bucket}/streamify/checkpoints/auth_events"
)


# Write to OSS in Parquet format
# month/day/hour are Spark partition fields
# The final output generated on OSS will be:
# streamify/auth_events/
# month=9/
#   day=28/
#       hour=...
query = (
    auth_events.writeStream
    .format("parquet")
    .partitionBy("month", "day", "hour")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)


# Spark Streaming Will Running all the time
# Ctrl + C can stop it
query.awaitTermination()

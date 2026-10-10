import os

from pyspark.sql import SparkSession
from streaming_functions import create_kafka_read_stream, process_stream
from schema import schema



# Create SparkSession
spark = (
    SparkSession.builder
    .appName("Process Page View Events To OSS")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")



# Configure Spark to access Alibaba Cloud OSS
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


# Read page_view_events from Kafka

page_view_events = create_kafka_read_stream(
    spark=spark,
    kafka_address=os.getenv("KAFKA_HOST","localhost"),
    kafka_port=os.getenv("KAFKA_PORT","9092"),
    topic="page_view_events",
    starting_offset="earliest"
)



# Parse Kafka JSON according to the Schema
page_view_events = process_stream(
    page_view_events,
    schema["page_view_events"],
    "page_view_events"
)



# Define the output of OSS
bucket = os.environ["OSS_BUCKET"]

output_path = (
    f"oss://{bucket}/streamify/page_view_events"
)

checkpoint_path = (
    f"oss://{bucket}/streamify/checkpoints/page_view_events"
)



# 6. Write in Parquet，Partition by month/day/hour

query = (
    page_view_events.writeStream
    .format("parquet")
    .partitionBy("month", "day", "hour")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

# Streaming keep running，Ctrl+C stop it
query.awaitTermination()

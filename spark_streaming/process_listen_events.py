import os

from pyspark.sql import SparkSession

from streaming_functions import create_kafka_read_stream, process_stream
from schema import schema


# Create SparkSession
spark = (
    SparkSession.builder
    .appName("Process Listen Events To OSS")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Configure Alibaba Cloud OSS
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


# Gain The Listen_events From Kafka
listen_events = create_kafka_read_stream(
    spark=spark,
    kafka_address="localhost",
    kafka_port="9092",
    topic="listen_events",
    starting_offset="earliest"
)


# Analyse And Clean The Data
listen_events = process_stream(
    listen_events,
    schema["listen_events"],
    "listen_events"
)


#  OSS Output Path
bucket = os.environ["OSS_BUCKET"]

output_path = (
    f"oss://{bucket}/streamify/listen_events"
)

checkpoint_path = (
    f"oss://{bucket}/streamify/checkpoints/listen_events"
)


print("OSS output path:")
print(output_path)

print("OSS checkpoint path:")
print(checkpoint_path)


# Write In Parquet
query = (
    listen_events.writeStream
    .format("parquet")
    .partitionBy(
        "month",
        "day",
        "hour"
    )
    .option(
        "path",
        output_path
    )
    .option(
        "checkpointLocation",
        checkpoint_path
    )
    .outputMode("append")
    .trigger(
        processingTime="30 seconds"
    )
    .start()
)


# Keep Running Streaming
query.awaitTermination()

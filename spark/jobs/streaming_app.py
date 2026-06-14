import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    from_json,
    lit,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    to_timestamp,
    window,
    year,
    month,
    dayofmonth,
)
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType


schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("service", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("method", StringType(), False),
        StructField("status_code", IntegerType(), False),
        StructField("is_error", BooleanType(), False),
        StructField("latency_ms", IntegerType(), False),
        StructField("request_size_bytes", IntegerType(), False),
        StructField("response_size_bytes", IntegerType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("trace_id", StringType(), False),
        StructField("host", StringType(), False),
    ]
)


def getenv(name: str, default: str) -> str:
    return os.getenv(name, default)


def write_aggregates_to_cassandra(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    keyspace = getenv("CASSANDRA_KEYSPACE", "microservice_metrics")

    prepared = (
        batch_df.withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .withColumn("window_start_text", date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("window_end_text", date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("batch_id", lit(batch_id))
        .drop("window")
    )

    (
        prepared.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="service_metrics", keyspace=keyspace)
        .save()
    )


def main() -> None:
    kafka_bootstrap_servers = getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = getenv("KAFKA_TOPIC", "microservice_logs")
    hdfs_raw_path = getenv("HDFS_RAW_PATH", "hdfs://namenode:8020/hdfs/logs")
    raw_checkpoint = getenv("HDFS_CHECKPOINT_RAW", "hdfs://namenode:8020/hdfs/checkpoints/raw")
    aggregate_checkpoint = getenv(
        "HDFS_CHECKPOINT_AGG", "hdfs://namenode:8020/hdfs/checkpoints/aggregates"
    )
    cassandra_host = getenv("CASSANDRA_HOST", "cassandra")

    spark = (
        SparkSession.builder.appName("MicroserviceLogStreaming")
        .config("spark.cassandra.connection.host", cassandra_host)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        kafka_stream.select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withColumn("year", year(col("event_time")))
        .withColumn("month", month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))
    )

    raw_query = (
        events.writeStream.format("parquet")
        .option("path", hdfs_raw_path)
        .option("checkpointLocation", raw_checkpoint)
        .partitionBy("year", "month", "day")
        .outputMode("append")
        .start()
    )

    aggregates = (
        events.withWatermark("event_time", "1 minute")
        .groupBy(window(col("event_time"), "30 seconds"), col("service"))
        .agg(
            count("*").alias("request_count"),
            spark_sum(col("is_error").cast("int")).alias("error_count"),
            avg("latency_ms").alias("avg_latency_ms"),
            spark_min("latency_ms").alias("min_latency_ms"),
            spark_max("latency_ms").alias("max_latency_ms"),
            avg("request_size_bytes").alias("avg_request_size_bytes"),
            avg("response_size_bytes").alias("avg_response_size_bytes"),
        )
        .withColumn("error_rate", col("error_count") / col("request_count"))
        .withColumn("requests_per_second", col("request_count") / lit(30.0))
    )

    aggregate_query = (
        aggregates.writeStream.foreachBatch(write_aggregates_to_cassandra)
        .option("checkpointLocation", aggregate_checkpoint)
        .outputMode("update")
        .start()
    )

    spark.streams.awaitAnyTermination()
    raw_query.awaitTermination()
    aggregate_query.awaitTermination()


if __name__ == "__main__":
    main()

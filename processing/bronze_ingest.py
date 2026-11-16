import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    to_date
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka_broker:29092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

BUCKET_NAME = "boxoffice-lake"
TOPIC_NAME = "raw.cinema.seat_snapshots"
BRONZE_OUTPUT_PATH = f"s3a://{BUCKET_NAME}/bronze/seat_snapshots/"
CHECKPOINT_PATH = f"s3a://{BUCKET_NAME}/checkpoints/bronze/"


def create_spark_session() -> SparkSession:
    """
    Initializes SparkSession with required Kafka and AWS S3A (MinIO) JAR packages.
    """
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]

    spark = (
        SparkSession.builder
        .appName("BoxOffice-Bronze-Kafka-To-MinIO")
        .config("spark.jars.packages", ",".join(packages))
        # MinIO S3A Configuration
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.bucket.create.enabled", "true")
        .config("spark.hadoop.fs.s3a.change.detection.enabled", "false")
        # Write-side optimization to mitigate the Small File Problem
        .config("spark.sql.files.maxRecordsPerFile", 50000)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_bronze_stream():
    spark = create_spark_session()

    # Minimal schema solely to extract partitioning keys for the Bronze Lakehouse
    partition_extract_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("show_id", StringType(), True),
        StructField("city_code", StringType(), True),
        StructField("snapshot_timestamp", StringType(), True)
    ])

    print(f"📡 Connecting to Kafka Broker at {KAFKA_BOOTSTRAP_SERVERS}...")

    # 1. Read raw stream from Kafka
    raw_kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Append ingestion metadata & extract partition columns (event_date, city_code)
    bronze_transformed = (
        raw_kafka_stream
        .selectExpr(
            "CAST(key AS STRING) AS show_id_key",
            "CAST(value AS STRING) AS raw_json_payload",
            "topic AS kafka_topic",
            "partition AS kafka_partition",
            "offset AS kafka_offset",
            "timestamp AS kafka_timestamp"
        )
        .withColumn("ingested_at_utc", current_timestamp())
        .withColumn("parsed_meta", from_json(col("raw_json_payload"), partition_extract_schema))
        .withColumn("city_code", col("parsed_meta.city_code"))
        .withColumn("event_date", to_date(col("parsed_meta.snapshot_timestamp")))
        .drop("parsed_meta")
    )

    print(f"🚀 Writing Bronze micro-batches to {BRONZE_OUTPUT_PATH}...")

    # 3. Write stream to MinIO in Parquet format partitioned by event_date and city_code
    query = (
        bronze_transformed.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", BRONZE_OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("event_date", "city_code")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run_bronze_stream()
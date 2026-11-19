import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, sha2, concat, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# --- Environment Configurations ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka_broker:29092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

BUCKET_NAME = "boxoffice-lake"
TOPIC_NAME = "raw.cinema.seat_snapshots"
SILVER_OUTPUT_PATH = f"s3a://{BUCKET_NAME}/silver/seat_snapshots/"
CHECKPOINT_PATH = f"s3a://{BUCKET_NAME}/checkpoints/silver/"

def create_spark_session() -> SparkSession:
    # Adding Delta Lake dependencies alongside Kafka and AWS S3A
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "io.delta:delta-spark_2.12:3.1.0"
    ]
    spark = (
        SparkSession.builder
        .appName("BoxOffice-Silver-Transform")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.shuffle.partitions", "6")
        # Delta Lake configurations
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO S3A configurations
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def run_silver_stream():
    spark = create_spark_session()

    # 1. Define the complete nested JSON schema
    category_schema = StructType([
        StructField("category_name", StringType(), True),
        StructField("base_price_inr", DoubleType(), True),
        StructField("total_seats", IntegerType(), True),
        StructField("booked_seats", IntegerType(), True),
        StructField("blocked_seats", IntegerType(), True)
        # Note: We can intentionally drop seat_states_bitmask here to save storage in Silver
    ])

    full_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("show_id", StringType(), True),
        StructField("theatre_id", StringType(), True),
        StructField("city_code", StringType(), True),
        StructField("snapshot_window", StringType(), True),
        StructField("snapshot_timestamp", StringType(), True), # Comes in as ISO string
        StructField("movie_id", StringType(), True),
        StructField("screen_id", StringType(), True),
        StructField("seat_categories", ArrayType(category_schema), True)
    ])

    # 2. Consume from Kafka
    raw_kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50000) # OOM protection
        .load()
    )

    # 3. Parse JSON and Cast Timestamp for Watermarking
    parsed_stream = (
        raw_kafka_stream
        .selectExpr("CAST(value AS STRING) as json_payload", "timestamp as kafka_timestamp")
        .withColumn("data", from_json(col("json_payload"), full_schema))
        .select("data.*", "kafka_timestamp")
        # Crucial: Cast string to actual TIMESTAMP data type so Spark can apply the watermark
        .withColumn("event_time", to_timestamp(col("snapshot_timestamp")))
        .withWatermark("event_time", "15 minutes")
    )

    # 4. Explode Arrays and Apply SHA-256 Deduplication Hash
    flattened_silver = (
        parsed_stream
        .select(
            col("event_id"),
            col("show_id"),
            col("theatre_id"),
            col("city_code"),
            col("movie_id"),
            col("snapshot_window"),
            col("event_time"),
            explode(col("seat_categories")).alias("category")
        )
        .withColumn("category_name", col("category.category_name"))
        .withColumn("base_price_inr", col("category.base_price_inr"))
        .withColumn("total_seats", col("category.total_seats"))
        .withColumn("booked_seats", col("category.booked_seats"))
        .withColumn("blocked_seats", col("category.blocked_seats"))
        # Deterministic Hash: If the same show, window, and category arrive twice, the hash is identical
        .withColumn("dedup_hash", sha2(concat(col("show_id"), col("snapshot_window"), col("category_name")), 256))
        .drop("category")
    )

    # 5. Drop Duplicates within the 15-minute watermark window
    deduplicated_silver = flattened_silver.dropDuplicatesWithinWatermark(["dedup_hash"])

    # 6. Write to Delta Lake on MinIO
# --- NEW FOREACHBATCH SINK ---
    def write_to_lake_and_warehouse(batch_df, batch_id):
        # 1. Write the batch to MinIO (Delta Lake)
        batch_df.write \
            .format("delta") \
            .mode("append") \
            .save("s3a://boxoffice-lake/silver/seat_snapshots/")
        
        # 2. Collect rows locally (safe for small 30-second micro-batches)
        rows = batch_df.collect()
        if not rows:
            return
            
        import psycopg2
        import psycopg2.extras
        
        # Connect to PostgreSQL container
        conn = psycopg2.connect(
            host="postgres_warehouse", 
            database="boxoffice", 
            user="postgres", 
            password="password123"
        )
        cursor = conn.cursor()
        
        # The Idempotent Upsert Query (ON CONFLICT DO UPDATE)
        insert_query = """
            INSERT INTO staging_seat_snapshots (
                dedup_hash, show_id, movie_id, theatre_id, city_code,
                snapshot_window, category_name, base_price_inr,
                booked_seats, tracked_gross_inr
            ) VALUES %s
            ON CONFLICT (dedup_hash) DO UPDATE SET
                movie_id = EXCLUDED.movie_id,
                theatre_id = EXCLUDED.theatre_id,
                booked_seats = EXCLUDED.booked_seats,
                tracked_gross_inr = EXCLUDED.tracked_gross_inr,
                updated_at = CURRENT_TIMESTAMP;
        """
        
        data = [
            (
                row.dedup_hash,
                row.show_id,
                row.movie_id,
                row.theatre_id,
                row.city_code,
                row.snapshot_window,
                row.category_name,
                row.base_price_inr,
                row.booked_seats,
                float(row.base_price_inr * row.booked_seats)
            ) for row in rows
        ]
        
        # Execute the bulk upsert and commit
        psycopg2.extras.execute_values(cursor, insert_query, data)
        conn.commit()
        cursor.close()
        conn.close()

    # Start the streaming query with the foreachBatch function
    query = (
        deduplicated_silver.writeStream
        .foreachBatch(write_to_lake_and_warehouse)
        .outputMode("update")
        .option("checkpointLocation", "s3a://boxoffice-lake/checkpoints/silver/")
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    query.awaitTermination()

if __name__ == "__main__":
    run_silver_stream()
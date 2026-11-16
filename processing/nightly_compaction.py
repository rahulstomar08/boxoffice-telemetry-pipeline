import os
from pyspark.sql import SparkSession

# --- Environment Configurations ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
SILVER_OUTPUT_PATH = "s3a://boxoffice-lake/silver/seat_snapshots/"

def run_compaction():
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "io.delta:delta-spark_2.12:3.1.0"
    ]
    
    spark = (
        SparkSession.builder
        .appName("BoxOffice-Nightly-Compaction")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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

    print(f"Starting OPTIMIZE on {SILVER_OUTPUT_PATH}...")
    optimize_query = f"OPTIMIZE delta.`{SILVER_OUTPUT_PATH}` ZORDER BY (movie_id, theatre_id)"
    spark.sql(optimize_query).show(truncate=False)
    
    print(f"Starting VACUUM on {SILVER_OUTPUT_PATH}...")
    vacuum_query = f"VACUUM delta.`{SILVER_OUTPUT_PATH}` RETAIN 168 HOURS"
    spark.sql(vacuum_query).show(truncate=False)
    
    print("Nightly Lakehouse Compaction Completed Successfully.")
    spark.stop()

if __name__ == "__main__":
    run_compaction()
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    input_file_name,
    substring
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType
)
from datetime import date

# =========================================================
# Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("Reddit_Bronze_Ingestion")

    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")

    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "2g")

    .config("spark.executor.cores", "2")

    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")

    .config("spark.sql.files.maxPartitionBytes", "64MB")

    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# Metadata
# =========================================================

ingestion_date = date.today().strftime("%Y-%m-%d")

raw_base_path = "s3a://reddit-comments-analysis-pipeline/raw/"
bronze_base_path = "s3a://reddit-comments-analysis-pipeline/bronze/"

# =========================================================
# Schemas
# =========================================================

submission_schema = StructType([
    StructField("post_id", StringType(), True),
    StructField("author", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("score", IntegerType(), True)
])

users_schema = StructType([
    StructField("username", StringType(), True)
])

user_relations_schema = StructType([
    StructField("source_author", StringType(), True),
    StructField("target_author", StringType(), True),
    StructField("sentiment_sum", LongType(), True),
    StructField("interaction_count", IntegerType(), True)
])

# =========================================================
# Dataset Config
# =========================================================

datasets = {
    "submissions": {
        "input_path": f"{raw_base_path}submissions/",
        "schema": submission_schema
    },

    "users": {
        "input_path": f"{raw_base_path}users/",
        "schema": users_schema
    },

    "user_relations": {
        "input_path": f"{raw_base_path}user_relations/",
        "schema": user_relations_schema
    }
}

# =========================================================
# Bronze Ingestion
# =========================================================

for table_name, config in datasets.items():

    print(f"\n==============================")
    print(f"Starting ingestion: {table_name}")
    print(f"==============================")

    input_path = config["input_path"]
    schema = config["schema"]

    # -----------------------------------------------------
    # Read All Chunked CSV Files
    # -----------------------------------------------------

    df = (
        spark.read
        .schema(schema)
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(input_path)
    )

    # -----------------------------------------------------
    # Metadata Columns
    # -----------------------------------------------------

    df = (
        df.withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("ingestion_date", lit(ingestion_date))
          .withColumn("source_file", input_file_name())
    )

    output_path = f"{bronze_base_path}{table_name}/"

    # -----------------------------------------------------
    # Simple Bronze Write
    # -----------------------------------------------------

    (
        df.write
        .format("delta")
        .mode("append")
        .save(output_path)
    )

    print(f"Finished Bronze ingestion: {table_name}")

# =========================================================
# Stop Spark
# =========================================================

spark.stop()
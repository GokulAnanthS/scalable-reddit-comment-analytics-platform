import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType
)

# =========================================================
# Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("Landing Ingestion")

    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "2g")

    .config("spark.executor.cores", "2")

    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")

    .config("spark.sql.files.maxPartitionBytes", "64MB")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

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
# Input Files
# =========================================================

files_path = {
    "submissions": {
        "path": "/opt/spark/data/submissions.csv",
        "schema": submission_schema,
        "chunks": 16
    },

    "users": {
        "path": "/opt/spark/data/users.csv",
        "schema": users_schema,
        "chunks": 4
    },

    "user_relations": {
        "path": "/opt/spark/data/user_relations.csv",
        "schema": user_relations_schema,
        "chunks": 12
    }
}

raw_base_path = "s3a://reddit-comments-analysis-pipeline/raw/"

# =========================================================
# RAW INGESTION
# =========================================================

for table_name, config in files_path.items():

    print(f"\n===================================")
    print(f"Processing: {table_name}")
    print(f"===================================")

    local_path = config["path"]
    schema = config["schema"]
    num_chunks = config["chunks"]

    # -----------------------------------------------------
    # Read CSV
    # -----------------------------------------------------

    df = (
        spark.read
        .schema(schema)
        .option("header", "true")
        .csv(local_path)
    )

    # -----------------------------------------------------
    # Split into Multiple Chunks
    # -----------------------------------------------------

    df = df.repartition(num_chunks)

    # -----------------------------------------------------
    # Write Chunked CSV Files to MinIO RAW
    # -----------------------------------------------------

    output_path = f"{raw_base_path}{table_name}/"

    (
        df.write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )

    print(f"{table_name} uploaded as multiple CSV chunks")

# =========================================================
# Stop Spark
# =========================================================

spark.stop()


# from pyspark.sql import SparkSession

# spark = (
#     SparkSession.builder
#     .appName("Landing Ingestion")
#     .config("spark.hadoop.fs.s3a.endpoint","http://minio:9000")
#     .config("spark.hadoop.fs.s3a.access.key","admin")
#     .config("spark.hadoop.fs.s3a.secret.key","password")
#     .config("spark.hadoop.fs.s3a.path.style.access","true")
#     .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
#     .getOrCreate()
# )

# files_path = {
#     "submissions":"/opt/spark/data/submissions.csv",
#     "users":"/opt/spark/data/users.csv",
#     "user_relations":"/opt/spark/data/user_relations.csv"
# }

# raw_path = "s3a://reddit-comments-analysis-pipeline/raw/"

# for table_name, local_path in files_path.items():
#     df = spark.read.option("header","true").option("inferSchema","true").csv(local_path)
#     (
#         df.write.mode("overwrite")
#         .option("header","true")
#         .csv(f"{raw_path}{table_name}/")
#     )

#     print(f"Raw file {table_name} stored in MinIO")
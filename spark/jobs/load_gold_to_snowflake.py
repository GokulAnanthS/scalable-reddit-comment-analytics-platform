from pyspark.sql import SparkSession
import os


spark = (
    SparkSession.builder
    .appName("Load_Gold_To_Snowflake")

    # MinIO Config
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))

    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Stability
    .config("spark.network.timeout", "600s")
    .config("spark.executor.heartbeatInterval", "60s")

    # Memory
    .config("spark.executor.memory", "5g")
    .config("spark.driver.memory", "5g")
    .config("spark.executor.cores", "1")

    # Performance
    .config("spark.sql.shuffle.partitions", "40")
    .config("spark.default.parallelism", "40")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")

    # Delta Lake
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    .getOrCreate()
)

# =========================================================
# Snowflake Config
# =========================================================

sfOptions = {
    "sfURL": os.getenv("SNOWFLAKE_URL"),
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "sfRole": os.getenv("SNOWFLAKE_ROLE")
}

# =========================================================
# Paths
# =========================================================

gold_base_path = "s3a://reddit-comments-analysis-pipeline/gold/"

tables = {
    "gold_subreddit_metrics": "GOLD_SUBREDDIT_METRICS",
    "gold_author_metrics": "GOLD_AUTHOR_METRICS",
    "gold_interaction_metrics": "GOLD_INTERACTION_METRICS",
    "gold_community_sentiment": "GOLD_COMMUNITY_SENTIMENT"
}


# =========================================================
# Connection Test
# =========================================================

try:

    print("===================================")
    print("Testing Snowflake Connection")
    print("===================================")

    test_df = (
        spark.read
        .format("snowflake")
        .options(**sfOptions)
        .option("query", "SELECT CURRENT_VERSION()")
        .load()
    )

    test_df.show(truncate=False)

    print("===================================")
    print("Snowflake Connection Successful")
    print("===================================")

except Exception as e:

    print("===================================")
    print("Snowflake Connection Failed")
    print("===================================")

    print(str(e))





# =========================================================
# Load Each Gold Table
# =========================================================

for path_name, snowflake_table in tables.items():

    print(f"Loading {path_name} into Snowflake")

    df = (
        spark.read
        .format("delta")
        .load(f"{gold_base_path}{path_name}/")
    )

    (
        df.write
        .format("snowflake")
        .options(**sfOptions)
        .option("dbtable", snowflake_table)
        .mode("overwrite")
        .save()
    )

    print(f"Finished loading {snowflake_table}")

spark.stop()
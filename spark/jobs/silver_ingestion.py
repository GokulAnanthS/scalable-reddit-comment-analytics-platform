import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,trim,lower,current_timestamp,year,month,when,lit,coalesce,row_number,max as spark_max)
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from delta.tables import DeltaTable

# =========================================================
# Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("Reddit_Silver_Incremental_Merge")

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
    .config("spark.sql.files.maxPartitionBytes", "128MB")

    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")

    # Delta
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# Paths
# =========================================================

bronze_base_path = "s3a://reddit-comments-analysis-pipeline/bronze/"
silver_base_path = "s3a://reddit-comments-analysis-pipeline/silver/"

# =========================================================
# Helper Function
# =========================================================

def get_latest_timestamp(silver_path):

    try:
        silver_df = (
            spark.read
            .format("delta")
            .load(silver_path)
        )
        latest_ts = (
            silver_df.agg(spark_max("ingestion_timestamp").alias("max_ts"))
            .collect()[0]["max_ts"]
        )
        return latest_ts
    except Exception:
        return None

# =========================================================
# SUBMISSIONS SILVER
# =========================================================

print("\n===================================")
print("Incremental Processing - submissions")
print("===================================")

submissions_bronze_path = f"{bronze_base_path}submissions/"
submissions_silver_path = f"{silver_base_path}submissions/"

# ---------------------------------------------------------
# Read Bronze
# ---------------------------------------------------------

submissions_df = (
    spark.read
    .format("delta")
    .load(submissions_bronze_path)
)

# ---------------------------------------------------------
# Incremental Filter
# ---------------------------------------------------------

latest_timestamp = get_latest_timestamp(submissions_silver_path)

if latest_timestamp:

    print(f"Latest Silver Timestamp: {latest_timestamp}")
    submissions_df = (
        submissions_df.filter(col("ingestion_timestamp") > lit(latest_timestamp))
    )

# ---------------------------------------------------------
# Check New Records
# ---------------------------------------------------------

new_rows = submissions_df.count()

print(f"New Bronze Rows: {new_rows:,}")

if new_rows > 0:

    # -----------------------------------------------------
    # Cleaning
    # -----------------------------------------------------

    window_spec = (
        Window.partitionBy("post_id")
        .orderBy(col("score").desc())
    )

    submissions_clean_df = (
        submissions_df
        .filter(col("post_id").isNotNull())
        .filter(col("subreddit").isNotNull())
        .filter(
            ~col("author").isin("[deleted]","[removed]","")
        )

        .withColumn("author",lower(trim(col("author"))))
        .withColumn("subreddit",lower(trim(col("subreddit"))))

        .withColumn("score",when(col("score") < 0, 0).otherwise(col("score")))

        .withColumn("score",coalesce(col("score").cast("long"), lit(0)))

        .repartition(20, "post_id")

        .withColumn("rn",row_number().over(window_spec))

        .filter(col("rn") == 1)

        .drop("rn")

        .withColumn("year",year(current_timestamp()))

        .withColumn("month",month(current_timestamp()))

        .withColumn("ingestion_ts",current_timestamp())

    )

    submissions_clean_df = (
        submissions_clean_df
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # -----------------------------------------------------
    # Initial Write or MERGE
    # -----------------------------------------------------

    if DeltaTable.isDeltaTable(spark, submissions_silver_path):

        print("Performing Incremental MERGE")
        delta_table = DeltaTable.forPath(spark,submissions_silver_path)
        (
            delta_table.alias("target")
            .merge(
                submissions_clean_df.alias("source"),
                "target.post_id = source.post_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:

        print("Creating Initial Silver Table")
        (
            submissions_clean_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("year", "month")
            .save(submissions_silver_path)
        )
    submissions_clean_df.unpersist()
    print("✅ Incremental submissions MERGE completed")

else:

    print("No New Bronze Records Found")

# =========================================================
# USERS SILVER
# =========================================================

print("\n===================================")
print("Incremental Processing - users")
print("===================================")

users_bronze_path = f"{bronze_base_path}users/"
users_silver_path = f"{silver_base_path}users/"

users_df = (
    spark.read
    .format("delta")
    .load(users_bronze_path)
)

latest_timestamp = get_latest_timestamp(users_silver_path)

if latest_timestamp:

    users_df = (users_df.filter(col("ingestion_timestamp") > lit(latest_timestamp)))

new_rows = users_df.count()

print(f"New Bronze Rows: {new_rows:,}")

if new_rows > 0:

    users_clean_df = (users_df
        .filter(col("username").isNotNull())
        .filter(trim(col("username")) != "")
        .withColumn("username",lower(trim(col("username"))))
        .repartition(10, "username")
        .withColumn("rn",row_number().over(Window.partitionBy("username").orderBy(lit(1))))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn("ingestion_ts",current_timestamp())
    )

    if DeltaTable.isDeltaTable(spark, users_silver_path):
        delta_table = DeltaTable.forPath(spark,users_silver_path)
        (
            delta_table.alias("target")
            .merge(users_clean_df.alias("source"),"target.username = source.username")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:

        (
            users_clean_df.write
            .format("delta")
            .mode("overwrite")
            .save(users_silver_path)
        )

    print("✅ Incremental users MERGE completed")

else:

    print("No New Bronze Records Found")

# =========================================================
# USER RELATIONS SILVER
# =========================================================

print("\n===================================")
print("Incremental Processing - user_relations")
print("===================================")

relations_bronze_path = f"{bronze_base_path}user_relations/"
relations_silver_path = f"{silver_base_path}user_relations/"

relations_df = (
    spark.read
    .format("delta")
    .load(relations_bronze_path)
)

latest_timestamp = get_latest_timestamp(relations_silver_path)

if latest_timestamp:

    relations_df = (
        relations_df
        .filter(col("ingestion_timestamp") > lit(latest_timestamp))
    )

new_rows = relations_df.count()

print(f"New Bronze Rows: {new_rows:,}")

if new_rows > 0:

    rel_window = (Window.partitionBy("source_author","target_author")
        .orderBy(col("interaction_count").desc())
    )

    relations_clean_df = (

        relations_df
        .filter(col("source_author").isNotNull())
        .filter(col("target_author").isNotNull())
        .withColumn("source_author",lower(trim(col("source_author"))))
        .withColumn("target_author",lower(trim(col("target_author"))))
        .filter(col("source_author") != col("target_author"))
        .withColumn("sentiment_sum",
            coalesce(
                col("sentiment_sum").cast("double"),lit(0.0)
            )
        )
        .withColumn("interaction_count",
            coalesce(
                col("interaction_count").cast("long"),lit(1)
            )
        )
        .repartition(40,"source_author","target_author")
        .withColumn("rn",row_number().over(rel_window))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn("ingestion_ts",current_timestamp())
    )

    if DeltaTable.isDeltaTable(spark, relations_silver_path):

        delta_table = DeltaTable.forPath(spark,relations_silver_path)

        (
            delta_table.alias("target")
            .merge(
                relations_clean_df.alias("source"),
                """
                target.source_author = source.source_author
                AND
                target.target_author = source.target_author
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:

        (
            relations_clean_df.write
            .format("delta")
            .mode("overwrite")
            .save(relations_silver_path)
        )

    print("✅ Incremental user_relations MERGE completed")

else:

    print("No New Bronze Records Found")

# =========================================================
# Stop Spark
# =========================================================

spark.stop()

print("\n🎉 ALL INCREMENTAL SILVER MERGES COMPLETED!")
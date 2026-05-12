from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, current_timestamp, year, month,
    when, lit, coalesce, row_number
)
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

# =========================================================
# Spark Session
# =========================================================
spark = (
    SparkSession.builder
    .appName("Reddit_Silver_Layer_Optimized")

    # MinIO Config
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Stability
    .config("spark.network.timeout", "600s")
    .config("spark.executor.heartbeatInterval", "60s")

    # Memory & Resources
    .config("spark.executor.memory", "5g")      # Increased slightly
    .config("spark.driver.memory", "5g")
    .config("spark.executor.cores", "1")

    # Shuffle & Performance
    .config("spark.sql.shuffle.partitions", "40")      # Better for your data
    .config("spark.default.parallelism", "40")
    .config("spark.sql.files.maxPartitionBytes", "128MB")
    .config("spark.sql.adaptive.enabled", "true")                    # Very Important
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")

    # Delta Lake
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

def print_row_count(df, stage):
    count = df.count()
    print(f"Rows after {stage}: {count:,}")
    return df

# =========================================================
# SUBMISSIONS SILVER
# =========================================================
print("\n===================================")
print("Processing submissions Silver Layer")
print("===================================")

submissions_bronze_path = f"{bronze_base_path}submissions/"
submissions_silver_path = f"{silver_base_path}submissions/"

submissions_df = spark.read.format("delta").load(submissions_bronze_path)
print_row_count(submissions_df, "Bronze Read")

# Cleaning + Deduplication using row_number (more efficient)
window_spec = Window.partitionBy("post_id").orderBy(col("score").desc())

submissions_clean_df = (
    submissions_df
    .filter(col("post_id").isNotNull())
    .filter(col("subreddit").isNotNull())
    .filter(~col("author").isin("[deleted]", "[removed]", ""))
    .withColumn("author", lower(trim(col("author"))))
    .withColumn("subreddit", lower(trim(col("subreddit"))))
    .withColumn("score", when(col("score") < 0, 0).otherwise(col("score")))
    .withColumn("score", coalesce(col("score").cast("long"), lit(0)))
    .repartition(20, "post_id")                    # Repartition by dedup key
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
    .withColumn("year", year(current_timestamp()))
    .withColumn("month", month(current_timestamp()))
    .withColumn("ingestion_ts", current_timestamp())
)

print_row_count(submissions_clean_df, "Cleaning + Deduplication")

submissions_clean_df = submissions_clean_df.persist(StorageLevel.MEMORY_AND_DISK)

(
    submissions_clean_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year", "month")
    .save(submissions_silver_path)
)

submissions_clean_df.unpersist()
print("✅ Finished submissions Silver layer\n")

# =========================================================
# USERS SILVER
# =========================================================
print("===================================")
print("Processing users Silver Layer")
print("===================================")

users_bronze_path = f"{bronze_base_path}users/"
users_silver_path = f"{silver_base_path}users/"

users_df = spark.read.format("delta").load(users_bronze_path)
print_row_count(users_df, "Bronze Read")

users_clean_df = (
    users_df
    .filter(col("username").isNotNull())
    .filter(trim(col("username")) != "")
    .withColumn("username", lower(trim(col("username"))))
    .repartition(10, "username")
    .withColumn("rn", row_number().over(Window.partitionBy("username").orderBy(lit(1))))
    .filter(col("rn") == 1)
    .drop("rn")
    .withColumn("ingestion_ts", current_timestamp())
)

print_row_count(users_clean_df, "Cleaning + Deduplication")

users_clean_df = users_clean_df.persist(StorageLevel.MEMORY_AND_DISK)

users_clean_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(users_silver_path)

users_clean_df.unpersist()
print("✅ Finished users Silver layer\n")

# =========================================================
# USER RELATIONS SILVER
# =========================================================
print("===================================")
print("Processing user_relations Silver Layer")
print("===================================")

relations_bronze_path = f"{bronze_base_path}user_relations/"
relations_silver_path = f"{silver_base_path}user_relations/"

relations_df = spark.read.format("delta").load(relations_bronze_path)
print_row_count(relations_df, "Bronze Read")

rel_window = Window.partitionBy("source_author", "target_author").orderBy(col("interaction_count").desc())

relations_clean_df = (
    relations_df
    .filter(col("source_author").isNotNull())
    .filter(col("target_author").isNotNull())
    .filter(col("source_author") != col("target_author"))
    .withColumn("source_author", lower(trim(col("source_author"))))
    .withColumn("target_author", lower(trim(col("target_author"))))
    .withColumn("sentiment_sum", coalesce(col("sentiment_sum").cast("double"), lit(0.0)))
    .withColumn("interaction_count", coalesce(col("interaction_count").cast("long"), lit(1)))
    .repartition(80, "source_author", "target_author")     # Important for big table
    .withColumn("rn", row_number().over(rel_window))
    .filter(col("rn") == 1)
    .drop("rn")
    .withColumn("ingestion_ts", current_timestamp())
)

print_row_count(relations_clean_df, "Cleaning + Deduplication")

relations_clean_df = relations_clean_df.persist(StorageLevel.MEMORY_AND_DISK)

relations_clean_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(relations_silver_path)

relations_clean_df.unpersist()
print("✅ Finished user_relations Silver layer\n")

# =========================================================
# Stop Spark
# =========================================================
spark.stop()
print("🎉 All Silver layers completed successfully!")
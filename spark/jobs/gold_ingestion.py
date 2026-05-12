from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,count,countDistinct,avg,max,sum as spark_sum,round,dense_rank,current_timestamp,lower,trim)
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

# =========================================================
# Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("Reddit_Gold_Layer")

    # MinIO Config
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")

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

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# Paths
# =========================================================

silver_base_path = "s3a://reddit-comments-analysis-pipeline/silver/"
gold_base_path = "s3a://reddit-comments-analysis-pipeline/gold/"

# =========================================================
# Read Silver Tables
# =========================================================

print("\n===================================")
print("Reading Silver Tables")
print("===================================")

submissions_path = f"{silver_base_path}submissions/"
users_path = f"{silver_base_path}users/"
relations_path = f"{silver_base_path}user_relations/"

submissions_df = (
    spark.read
    .format("delta")
    .load(submissions_path)
)

users_df = (
    spark.read
    .format("delta")
    .load(users_path)
)

relations_df = (
    spark.read
    .format("delta")
    .load(relations_path)
)

print(f"Submissions Rows : {submissions_df.count():,}")
print(f"Users Rows       : {users_df.count():,}")
print(f"Relations Rows   : {relations_df.count():,}")

# =========================================================
# Persist Important Tables
# =========================================================

submissions_df = submissions_df.persist(StorageLevel.MEMORY_AND_DISK)
relations_df = relations_df.persist(StorageLevel.MEMORY_AND_DISK)

# =========================================================
# GOLD 1 — SUBREDDIT METRICS
# =========================================================

print("\n===================================")
print("Building gold_subreddit_metrics")
print("===================================")

subreddit_metrics_df = (

    submissions_df
    .groupBy("subreddit")
    .agg(
        count("post_id").alias("total_posts"),
        countDistinct("author").alias("unique_authors"),
        round(avg("score"), 2).alias("avg_score"),
        max("score").alias("max_score"),
        spark_sum("score").alias("total_score")
    )
    .withColumn(
        "subreddit_rank",
        dense_rank().over(
            Window.orderBy(col("total_posts").desc())
        )
    )
    .withColumn(
        "gold_generated_ts",
        current_timestamp()
    )
)

subreddit_metrics_output = f"{gold_base_path}gold_subreddit_metrics/"
(
    subreddit_metrics_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(subreddit_metrics_output)
)

print("✅ Finished gold_subreddit_metrics")

# =========================================================
# GOLD 2 — AUTHOR METRICS
# =========================================================

print("\n===================================")
print("Building gold_author_metrics")
print("===================================")
author_metrics_df = (
    submissions_df
    .groupBy("author")
    .agg(
        count("post_id").alias("total_posts"),
        countDistinct("subreddit").alias("subreddit_count"),
        round(avg("score"), 2).alias("avg_score"),
        max("score").alias("max_score"),
        spark_sum("score").alias("total_score")
    )
    .withColumn(
        "author_rank",
        dense_rank().over(
            Window.orderBy(col("total_score").desc())
        )
    )
    .withColumn(
        "gold_generated_ts",
        current_timestamp()
    )
)

author_metrics_output = f"{gold_base_path}gold_author_metrics/"
(
    author_metrics_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(author_metrics_output)
)

print("✅ Finished gold_author_metrics")

# =========================================================
# GOLD 3 — USER INTERACTION METRICS
# =========================================================

print("\n===================================")
print("Building gold_interaction_metrics")
print("===================================")

interaction_metrics_df = (
    relations_df
    .groupBy("source_author")
    .agg(
        countDistinct("target_author").alias("unique_interactions"),
        spark_sum("interaction_count").alias("total_interactions"),
        round(avg("sentiment_sum"), 2).alias("avg_sentiment"),
        spark_sum("sentiment_sum").alias("total_sentiment")
    )
    .withColumn(
        "interaction_rank",
        dense_rank().over(
            Window.orderBy(col("total_interactions").desc())
        )
    )
    .withColumn(
        "gold_generated_ts",
        current_timestamp()
    )
)

interaction_metrics_output = f"{gold_base_path}gold_interaction_metrics/"
(
    interaction_metrics_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(interaction_metrics_output)
)

print("✅ Finished gold_interaction_metrics")

# =========================================================
# GOLD 4 — COMMUNITY SENTIMENT METRICS
# =========================================================

print("\n===================================")
print("Building gold_community_sentiment")
print("===================================")

community_sentiment_df = (
    submissions_df.alias("s")
    .join(
        relations_df.alias("r"),
        lower(trim(col("s.author"))) == lower(trim(col("r.source_author"))),
        "inner"
    )
    .groupBy("s.subreddit")
    .agg(
        countDistinct("s.author").alias("active_authors"),
        spark_sum("r.interaction_count").alias("community_interactions"),
        round(avg("r.sentiment_sum"), 2).alias("avg_sentiment"),
        spark_sum("r.sentiment_sum").alias("total_sentiment")
    )
    .withColumn(
        "community_rank",
        dense_rank().over(
            Window.orderBy(col("community_interactions").desc())
        )
    )
    .withColumn(
        "gold_generated_ts",
        current_timestamp()
    )
)

community_sentiment_output = f"{gold_base_path}gold_community_sentiment/"
(
    community_sentiment_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(community_sentiment_output)
)

print("✅ Finished gold_community_sentiment")

# =========================================================
# Cleanup
# =========================================================

submissions_df.unpersist()
relations_df.unpersist()

# =========================================================
# Stop Spark
# =========================================================

spark.stop()

print("\n🎉 ALL GOLD LAYERS COMPLETED SUCCESSFULLY!")
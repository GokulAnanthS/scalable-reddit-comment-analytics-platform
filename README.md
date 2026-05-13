# Reddit Lakehouse Pipeline

## Overview

This project is a production-style end-to-end Reddit data engineering pipeline built using Apache Spark, Delta Lake, Airflow, Snowflake, and dbt. The pipeline processes Reddit datasets through Bronze, Silver, and Gold Medallion layers and produces analytics-ready datasets for reporting and downstream analytics.

The project focuses on distributed data processing, incremental ETL pipelines, Delta Lake optimizations, orchestration, analytics engineering, and modern lakehouse architecture.

---

# Architecture

```text
Raw CSV Data
      ↓
Bronze Layer (Delta Lake)
      ↓
Silver Layer (Incremental MERGE + Cleaning)
      ↓
Gold Layer (Analytics Aggregations)
      ↓
Snowflake Warehouse
      ↓
dbt Models + Tests
```

---

# Tech Stack

## Core Technologies

* Apache Spark 3.5
* PySpark
* Delta Lake
* Apache Airflow
* Snowflake
* dbt
* Docker
* MinIO (S3-compatible object storage)
* Python 3.11

## Data Engineering Concepts

* Medallion Architecture
* Incremental Processing
* Delta MERGE Operations
* Distributed Data Processing
* Window Functions
* Deduplication Strategies
* Partition Optimization
* Analytics Engineering

---

# Project Structure

```text
reddit-pipeline/
│
├── dags/
│   └── airflow_dags/
│
├── spark_jobs/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── dbt_project/
│   ├── models/
│   ├── tests/
│   └── macros/
│
├── docker/
├── configs/
├── notebooks/
├── monitoring/
│
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .gitignore
```

---

# Datasets

The project processes three Reddit datasets.

## submissions

```text
post_id
author
subreddit
score
```

## users

```text
username
```

## user_relations

```text
source_author
target_author
sentiment_sum
interaction_count
```

---

# Medallion Architecture

## Bronze Layer

The Bronze layer ingests raw Reddit datasets into Delta Lake tables stored in MinIO object storage.

### Features

* Explicit schema enforcement
* Metadata tracking
* Chunked ingestion
* Append-only ingestion logic
* Delta Lake storage
* Distributed Spark processing

### Bronze Datasets

* submissions
* users
* user_relations

---

## Silver Layer

The Silver layer transforms raw Bronze data into cleaned and standardized datasets.

### Features

* Incremental Delta MERGE
* Deduplication using window functions
* Data normalization
* Partition optimization
* Adaptive query execution
* Incremental processing

### Key Transformations

* Removed null and invalid records
* Normalized usernames and subreddit names
* Filtered deleted users
* Deduplicated records using row_number()
* Optimized partitioning strategies
* Added ingestion metadata

---

## Gold Layer

The Gold layer produces analytics-ready aggregated datasets.

### Gold Tables

* gold_subreddit_metrics
* gold_author_metrics
* gold_interaction_metrics
* gold_community_sentiment

### Metrics Generated

* Total posts
* Average scores
* Unique authors
* Community sentiment
* Interaction metrics
* Author rankings
* Subreddit rankings

---

# Airflow Orchestration

Apache Airflow orchestrates the complete pipeline.

## Pipeline Flow

```text
Bronze Ingestion
      ↓
Silver Processing
      ↓
Gold Aggregations
      ↓
Snowflake Loading
      ↓
dbt Models
```

### Features

* DAG scheduling
* Retry handling
* Task dependencies
* Spark job orchestration
* Automated pipeline execution

---

# Snowflake Integration

Gold Delta tables are loaded into Snowflake using the Spark Snowflake Connector.

Snowflake is used for:

* Analytics warehousing
* SQL-based transformations
* dbt integration
* BI-ready analytics

---

# dbt Integration

The project uses dbt for analytics engineering on top of Snowflake.

### Features

* SQL modeling
* Source definitions
* Data tests
* Modular transformations
* Analytics-ready marts

### Implemented Tests

* not_null
* unique
* schema validation

---

# Spark Optimizations

The pipeline includes multiple distributed Spark optimization techniques.

### Optimizations Used

* Adaptive Query Execution (AQE)
* Partition pruning
* Repartitioning strategies
* Window-based deduplication
* MEMORY_AND_DISK persistence
* Incremental MERGE processing
* Shuffle optimization
* Delta Lake partitioning

---

# Example Gold Analytics

## gold_subreddit_metrics

Tracks subreddit-level engagement metrics.

### Columns

```text
subreddit
total_posts
unique_authors
avg_score
max_score
total_score
subreddit_rank
```

---

## gold_author_metrics

Tracks author engagement and scoring metrics.

### Columns

```text
author
total_posts
subreddit_count
avg_score
max_score
total_score
author_rank
```

---

# How to Run

## 1. Start Docker Services

```bash
docker-compose up -d
```

## 2. Run Bronze Layer

```bash
spark-submit spark_jobs/bronze/bronze_ingestion.py
```

## 3. Run Silver Layer

```bash
spark-submit spark_jobs/silver/silver_processing.py
```

## 4. Run Gold Layer

```bash
spark-submit spark_jobs/gold/gold_processing.py
```

## 5. Load Gold Tables into Snowflake

```bash
spark-submit spark_jobs/gold/load_gold_to_snowflake.py
```

## 6. Run dbt Models

```bash
dbt run
```

## 7. Run dbt Tests

```bash
dbt test
```

---

# Resume Highlights

* Built an end-to-end Reddit lakehouse pipeline using Apache Spark, Delta Lake, Airflow, Snowflake, and dbt.

* Implemented Bronze, Silver, and Gold Medallion architecture with incremental Delta MERGE processing.

* Developed distributed Spark transformations with partition optimization, window-based deduplication, and analytics-ready aggregations.

* Integrated Snowflake and dbt for analytics engineering, SQL modeling, and data validation.

---

# Future Improvements

Optional future enhancements:

* Grafana monitoring
* Prometheus metrics
* Great Expectations
* Terraform infrastructure
* NLP sentiment models
* Real-time streaming with Kafka
* AWS cloud migration

---

# .gitignore

```gitignore
# Python
__pycache__/
*.pyc
*.pyo
*.pyd
*.swp
*.tmp
*.temp

# Virtual Environment
venv/
.env/
.venv/

# Environment Variables
.env
.env.*

# Jupyter
.ipynb_checkpoints/

# Spark
metastore_db/
spark-warehouse/
derby.log

# Delta Lake
_delta_log/

# Airflow
airflow/logs/
airflow/airflow.db
airflow/webserver_config.py
airflow/airflow.cfg

# dbt
.dbt/
target/
dbt_packages/
logs/

# Docker
*.pid
*.log

# Mac
.DS_Store

# Windows
Thumbs.db
desktop.ini

# VSCode
.vscode/

# IntelliJ
.idea/

# Terraform
.terraform/
*.tfstate
*.tfstate.backup

# Data Files
*.csv
*.parquet
*.json
*.gz

# MinIO Local Storage
minio_data/

# Build Files
build/
dist/

# Secrets
secrets/
credentials/
```

---

# Final Status

```text
✅ Bronze Layer
✅ Silver Layer
✅ Gold Layer
✅ Incremental Delta MERGE
✅ Airflow Orchestration
✅ Snowflake Integration
✅ dbt Models + Tests
✅ End-to-End Lakehouse Pipeline
```

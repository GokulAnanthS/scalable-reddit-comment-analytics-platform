from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="gold_files_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    spark_task = SparkSubmitOperator(
        task_id="run_spark_gold_ingestion",
        application="/opt/spark/jobs/gold_ingestion.py",
        conn_id="spark_default",
        verbose=True,
        packages="io.delta:delta-spark_2.12:3.1.0,"
                 "org.apache.hadoop:hadoop-aws:3.3.4,"
                 "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )
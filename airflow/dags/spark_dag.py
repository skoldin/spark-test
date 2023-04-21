from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 4, 20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spark_jobs_dag",
    default_args=default_args,
    description="Test spark app orchestration",
    schedule_interval=timedelta(hours=1),
    catchup=False,
)

process_users = SparkSubmitOperator(
    task_id="process_users",
    application="/usr/local/spark/jobs/process_users.py",
    conn_id="spark_default",
    dag=dag,
)


process_videos = SparkSubmitOperator(
    task_id="process_videos",
    application="/usr/local/spark/jobs/process_videos.py",
    conn_id="spark_default",
    dag=dag,
)

process_events = SparkSubmitOperator(
    task_id="process_events",
    application="/usr/local/spark/jobs/process_events.py",
    conn_id="spark_default",
    dag=dag,
)

process_users >> process_videos >> process_events
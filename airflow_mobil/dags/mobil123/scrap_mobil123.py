import json
import os
import requests
import pendulum
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Add the 'scrapper' directory to the Python path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../scrape/script/mobil123")
    )
)
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../spark_transform/mobil123")
    )
)
from mobil123_scrape import mobil123_scrape


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 22),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "scrap_mobil123",
    default_args=default_args,
    description="Scrapper for mobil123 web",
    schedule_interval=timedelta(days=1),
)

scrape_task = PythonOperator(
    task_id="scrape_mobil123", python_callable=mobil123_scrape, dag=dag
)

modify_data = SparkSubmitOperator(
    task_id="modify_data",
    application="/home/miracle/mobil/spark_transform/mobil123.py",
    conn_id="spark_local",
    dag=dag,
)

scrape_task >> modify_data

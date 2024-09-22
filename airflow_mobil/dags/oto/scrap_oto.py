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

# Update sys.path to include directories
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../scrape/script/oto")
    )
)
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../spark_transform"))
)
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../store_db"))
)

print("Updated sys.path:")
for path in sys.path:
    print(path)

from oto_scrape import oto_scrape
from oto_store import save_to_postgres

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
    "scrap_oto_dag",
    default_args=default_args,
    description="Scrapper for oto web",
    schedule=timedelta(
        days=1
    ),  # Updated to use 'schedule' instead of 'schedule_interval'
)

scrape_task = PythonOperator(
    task_id="scrape_oto",
    python_callable=oto_scrape,
    dag=dag,
)

modify_data = SparkSubmitOperator(
    task_id="modify_data",
    application="/home/miracle/mobil/spark_transform/oto_transform.py",  # Path to the Spark application
    conn_id="spark_local",
    dag=dag,
)

save_task = PythonOperator(
    task_id="save_to_postgres",
    python_callable=save_to_postgres,
    dag=dag,
)

scrape_task >> modify_data >> save_task

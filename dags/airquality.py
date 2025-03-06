import json
import requests
from datetime import datetime, timedelta

from airflow.models import Variable
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_FILE_NAME = "/opt/airflow/dags/data.json"

def _get_airquality_data():
    API_KEY = Variable.get("air_quality_key")
    print(f"API Key: {API_KEY[:3]}******{API_KEY[-3:]}")

with DAG(
    "airquality_api_dag",
    schedule="*/10 * * * *",
    start_date=timezone.datetime(2025, 3, 6),
    tags=["capstone","i"]
):
    start = EmptyOperator(task_id="start")

    get_airquality_data = PythonOperator(
        task_id="get_airquality_data",
        python_callable=_get_airquality_data,
    )

    end = EmptyOperator(task_id="end")

    start >> get_airquality_data >> end
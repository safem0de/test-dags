import json
import requests
import os
# from datetime import datetime, timedelta

from airflow.models import Variable
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_FILE_PATH = "/opt/airflow/dags/"
CONN_STR = "0_postgres_db"

API_URL = Variable.get("air_quality_url")
API_KEY = Variable.get("air_quality_key")

## function

def create_file_if_not_exist(path: str, filename: str, data:str):
    """Create a file if it does not exist in the given path."""
    file_path = os.path.join(path, filename)
    
    try:
        parsed_data = json.loads(data)
    except json.JSONDecodeError:
        print("Error: Provided data is not valid JSON.")
        return

    if not os.path.exists(file_path):
        with open(file_path, 'w') as file:
            json.dump(data, parsed_data)
        print(f"File created: {file_path}")
    else:
        print(f"File already exists: {file_path}")


def sql_command(schema_name:str, sql_statement:str):
    pg_hook = PostgresHook(
        postgres_conn_id=CONN_STR,
        schema=schema_name
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = sql_statement

    cursor.execute(sql)
    connection.commit()
    

## Pipeline method
#### database
def _check_conn_string(conn_id: str):
    """
    ตรวจสอบว่าการเชื่อมต่อฐานข้อมูล PostgreSQL ใช้ conn_id ที่กำหนดสามารถเชื่อมต่อได้หรือไม่
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT 1;")
        connection.close()
        print(f"✅ Connection {conn_id} is valid!")
        return True
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        return False


def _create_aqi_database():
    sql_statement = """
        CREATE DATABASE IF NOT EXISTS aqi_database;
        )
    """
    sql_command("postgres", sql_statement)


def _create_aqi_table_location():
    sql_statement = """
        CREATE TABLE IF NOT EXISTS location (
        location_id INT AUTO_INCREMENT PRIMARY KEY,
        city VARCHAR(255) NOT NULL,
        state VARCHAR(255) NOT NULL,
        country VARCHAR(50) DEFAULT 'Thailand',
        latitude DECIMAL(10, 6),
        longitude DECIMAL(10, 6),
        UNIQUE (city, state, country));
        )
    """
    sql_command("aqi_database", sql_statement)

#### api
def _init_airquality_data():
    print(f"API Url: {API_URL}")
    print(f"API Key: {API_KEY[:3]}******{API_KEY[-3:]}")


def _get_state_data():
    # {{urlExternalAPI}}v2/states?country={{COUNTRY_NAME}}&key={{YOUR_API_KEY}}
    payload = {
        "country": "thailand",
        "key": API_KEY
    }

    url = f"{API_URL}v2/states"
    response = requests.get(url, params=payload)
    print(response.url)

    data = response.json()
    print(data)

    create_file_if_not_exist(DAG_FILE_PATH,"location_master",data)


with DAG(
    "airquality_database",
    schedule="*/5 * * * *",
    start_date=timezone.datetime(2025, 3, 6),
    tags=["capstone","database"]
):
    start = EmptyOperator(task_id="start")

    check_conn_string = PythonOperator(
        task_id="check_conn_string",
        python_callable=_check_conn_string,
    )

    create_aqi_database = PythonOperator(
        task_id="create_aqi_database",
        python_callable=_create_aqi_database,
    )

    create_aqi_table_location = PythonOperator(
        task_id="create_aqi_table_location",
        python_callable=_create_aqi_table_location,
    )

    init_airquality_data = PythonOperator(
        task_id="init_airquality_data",
        python_callable=_init_airquality_data,
    )

    get_state_data = PythonOperator(
        task_id="get_state_data",
        python_callable=_get_state_data,
    )

    end = EmptyOperator(task_id="end")

    start >> check_conn_string >> create_aqi_database >> create_aqi_table_location >> init_airquality_data >> get_state_data >> end
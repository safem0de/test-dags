import json
import requests
import os
from ratelimit import limits, sleep_and_retry

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

state_file_name = "state_master.json"

## function

def create_file_if_not_exist(path: str, filename: str, data: dict):
    """Create a file if it does not exist in the given path."""
    file_path = os.path.join(path, filename)
    
    if not isinstance(data, dict):  # เช็คให้แน่ใจว่า data เป็น dict
        print("Error: Provided data is not a valid JSON dictionary.")
        return

    if not os.path.exists(file_path):
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)  # บันทึกเป็นไฟล์ JSON ที่อ่านง่าย
        print(f"✅ File created: {file_path}")
    else:
        print(f"⚠️ File already exists: {file_path}")


def execute_sql(database_name: str, sql_statement: str):
    """
    Execute an SQL statement on the specified PostgreSQL database.
    """
    try:
        pg_hook = PostgresHook(
            postgres_conn_id=CONN_STR, 
            database=database_name  # เชื่อมต่อกับ database ที่กำหนด
        )
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        cursor.execute(sql_statement)
        connection.commit()

        cursor.close()
        connection.close()

        print(f"✅ SQL executed successfully on database: {database_name}")

    except Exception as e:
        print(f"❌ Error executing SQL on {database_name}: {str(e)}")



def check_conn_string(conn_id: str):
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
    

@sleep_and_retry
@limits(calls=5, period=60)  # จำกัด 5 ครั้ง/นาที
def fetch_api(url,params=None):
    """
    Fetch AQI data with optional query parameters.
    """
    try:
        response = requests.get(url, params=params)  # ✅ Support dynamic parameters
        response.raise_for_status()  # Raise error for bad responses (4xx, 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ API Request failed: {e}")
        return None
    

## Pipeline method
#### database

def _create_aqi_database():
    check_conn_string(CONN_STR)
    
    pg_hook = PostgresHook(postgres_conn_id=CONN_STR)
    connection = pg_hook.get_conn()
    connection.set_isolation_level(0)  # ปิด transaction block

    cursor = connection.cursor()

    # เช็คว่ามี database หรือยัง
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'aqi_database';")
    exists = cursor.fetchone()
    
    if not exists:
        cursor.execute("CREATE DATABASE aqi_database;")
        connection.commit()
    
    connection.close()


def _create_aqi_table_location():
    sql_statement = """
        CREATE TABLE IF NOT EXISTS location (
            location_id SERIAL PRIMARY KEY,
            city VARCHAR(255) NOT NULL,
            state VARCHAR(255) NOT NULL,
            country VARCHAR(50) DEFAULT 'Thailand',
            latitude DECIMAL(10, 6),
            longitude DECIMAL(10, 6),
            UNIQUE (city, state, country)
        );
    """
    execute_sql(database_name="aqi_database", sql_statement=sql_statement)


def _create_aqi_table_aqi_data():
    sql_statement = """
        CREATE TABLE IF NOT EXISTS aqi_data (
            aqi_id SERIAL PRIMARY KEY,
            location_id INT,
            timestamp DATETIME NOT NULL,
            aqius INT NOT NULL, 
            mainus VARCHAR(10),
            aqicn INT,
            maincn VARCHAR(10),
            FOREIGN KEY (location_id) REFERENCES location(location_id) ON DELETE CASCADE
        );
    """
    execute_sql(database_name="aqi_database", sql_statement=sql_statement)


def _create_aqi_table_weather_data():
    sql_statement = """
        CREATE TABLE IF NOT EXISTS weather_data (
            weather_id SERIAL PRIMARY KEY,
            location_id INT,
            timestamp DATETIME NOT NULL,
            temperature DECIMAL(5,2),
            pressure INT,
            humidity INT,
            wind_speed DECIMAL(5,2),
            wind_direction INT,
            FOREIGN KEY (location_id) REFERENCES location(location_id) ON DELETE CASCADE
        );
    """
    execute_sql(database_name="aqi_database", sql_statement=sql_statement)


#### api
def _init_airquality_data():
    print(f"API Url: {API_URL}")
    print(f"API Key: {API_KEY[:3]}******{API_KEY[-3:]}")


def _get_state_data():
    """
    Fetch state data from API only if the file does not exist.
    """
    file_name = state_file_name
    file_path = os.path.join(DAG_FILE_PATH, file_name)

    # ✅ If file exists, do nothing
    if os.path.exists(file_path):
        print(f"✅ File '{file_path}' already exists. Skipping API call.")
        return
    
    payload = {
        "country": "thailand",
        "key": API_KEY
    }

    # {{urlExternalAPI}}v2/states?country={{COUNTRY_NAME}}&key={{YOUR_API_KEY}}

    url = f"{API_URL}v2/states"
    # response = requests.get(url, params=payload)
    # print(response.url)
    # data = response.json()
    
    data = fetch_api(url,payload)
    print(data)

    create_file_if_not_exist(DAG_FILE_PATH, file_name, data)


def _get_city_data():
    with open(state_file_name, "r") as f:
        data = json.load(f)

    ## 

    file_name = f"city_master_{""}.json"
    file_path = os.path.join(DAG_FILE_PATH, file_name)

    # ✅ If file exists, do nothing
    if os.path.exists(file_path):
        print(f"✅ File '{file_path}' already exists. Skipping API call.")
        return

    payload = {
        "state": f"{""}",
        "country": "thailand",
        "key": API_KEY
    }
    
    #{{urlExternalAPI}}v2/city?city=Bang Bon&state=bangkok&country=thailand&key={{YOUR_API_KEY}}

    url = f"{API_URL}v2/cities"
    # response = requests.get(url, params=payload)
    # print(response.url)
    # data = response.json()

    data = fetch_api(url,payload)
    print(data)

    create_file_if_not_exist(DAG_FILE_PATH, file_name, data)

with DAG(
    "airquality_database",
    schedule=None,
    start_date=timezone.datetime(2025, 3, 6),
    tags=["capstone","database"]
):
    start = EmptyOperator(task_id="start")

    create_aqi_database = PythonOperator(
        task_id="create_aqi_database",
        python_callable=_create_aqi_database,
    )

    create_aqi_table_location = PythonOperator(
        task_id="create_aqi_table_location",
        python_callable=_create_aqi_table_location,
    )

    create_aqi_table_aqi_data = PythonOperator(
        task_id="create_aqi_table_aqi_data",
        python_callable=_create_aqi_table_aqi_data,
    )

    create_aqi_table_weather_data = PythonOperator(
        task_id="create_aqi_table_weather_data",
        python_callable=_create_aqi_table_weather_data,
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

    start >> create_aqi_database >> create_aqi_table_location >> create_aqi_table_aqi_data >> create_aqi_table_weather_data >> end
    start >> init_airquality_data >> get_state_data >> end
from dags.aqi_class.AirQualityDatabase import AirQualityDatabase

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

# ตั้งค่าพารามิเตอร์
conn_id = "0_postgres_db"
api_url = "https://api.waqi.info/"
api_key = "YOUR_API_KEY"
dag_file_path = "/opt/airflow/dags/"

state_file_name = "state_master.json"

# สร้าง Object สำหรับใช้งาน
aqi_db = AirQualityDatabase(conn_id, api_url, api_key, dag_file_path)

def _create_aqi_database():
    aqi_db.create_aqi_database()

def _create_aqi_table_location():
    aqi_db.create_aqi_table_location()

def _create_aqi_table_aqi_data():
    aqi_db.create_aqi_table_aqi_data()

def _create_aqi_table_weather_data():
    aqi_db.create_aqi_table_weather_data()

def _get_state_data():
    aqi_db.get_state_data()

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

    get_state_data = PythonOperator(
        task_id="get_state_data",
        python_callable=_get_state_data,
    )

    end = EmptyOperator(task_id="end")

    start >> create_aqi_database >> create_aqi_table_location >> create_aqi_table_aqi_data >> create_aqi_table_weather_data >> end
    start >> get_state_data >> end
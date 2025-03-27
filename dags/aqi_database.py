import os, sys, time
sys.path.append('/opt/airflow/dags/latest/dags')

from datetime import timedelta
from dags.aqi_class.AirQualityDatabase import AirQualityDatabase
from dags.aqi_class.CommonServices import CommonServices
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


# ตั้งค่าพารามิเตอร์
conn_id = "0_postgres_db"
api_url = Variable.get("air_quality_url")
api_key = Variable.get("air_quality_key_db")
api_key2 = Variable.get("air_quality_key_dwh")
api_keys = [api_key, api_key2]
dag_file_path = "/opt/airflow/dags/"

state_file_name = "state_master.json"

# เรียกใช้ service ที่จำเป็น
cms = CommonServices()
aqi_db = AirQualityDatabase(conn_id, api_url, api_keys, dag_file_path)

def _create_aqi_database():
    aqi_db.create_aqi_database()

# def _create_aqi_table_location():
#     aqi_db.create_aqi_table_location()

def _create_table_aqi_rawdata():
    aqi_db.create_table_aqi_rawdata()

# def _create_aqi_table_weather_data():
#     aqi_db.create_aqi_table_weather_data()

def _get_state_data():
    aqi_db.get_state_data()
    time.sleep(60) # reset api key

def _get_city_data():
    # ✅ สร้าง path ให้ถูกต้อง
    file_path = os.path.join(dag_file_path, state_file_name)

    # ✅ ตรวจสอบว่าไฟล์มีอยู่จริง
    if not os.path.exists(file_path):
        print(f"❌ Error: File '{file_path}' not found.")
        return

    state_list = cms.json_to_list(file_path, "data", "state")
    for st in state_list:
        aqi_db.get_city_data(st)

def _generate_state_city_region_csv():
    transform_output_filename = "transform_state_city_region.csv"
    aqi_db.generate_state_city_region_csv(dag_file_path,state_file_name,transform_output_filename)

with DAG(
    "airquality_database",
    schedule=None,
    start_date=timezone.datetime(2025, 3, 8),
    max_active_runs=1,  # ✅ จำกัดให้รันได้ครั้งละ 1 Task
    concurrency=1,      # ✅ จำกัดให้มี 1 Task ที่รัน API พร้อมกัน
    tags=["capstone","database"]
):
    start = EmptyOperator(task_id="start")

    create_aqi_database = PythonOperator(
        task_id="create_aqi_database",
        python_callable=_create_aqi_database,
    )

    # create_aqi_table_location = PythonOperator(
    #     task_id="create_aqi_table_location",
    #     python_callable=_create_aqi_table_location,
    # )

    create_table_aqi_rawdata = PythonOperator(
        task_id="create_table_aqi_rawdata",
        python_callable=_create_table_aqi_rawdata,
    )

    get_state_data = PythonOperator(
        task_id="get_state_data",
        python_callable=_get_state_data,
    )

    get_city_data = PythonOperator(
        task_id="get_city_data",
        python_callable=_get_city_data,
        retries=3,                              # ✅ ลองใหม่ 3 ครั้งถ้าล้มเหลว
        retry_delay=timedelta(seconds=20),      # ✅ รอ 20 sec ก่อน retry
        execution_timeout=timedelta(minutes=10),  # ✅ จำกัดเวลาทำงาน Task
        depends_on_past=True  # ✅ ต้องรอ Task ก่อนหน้าสำเร็จก่อนจึงทำงาน
    )

    generate_state_city_region_csv = PythonOperator(
        task_id="generate_state_city_region_csv",
        python_callable=_generate_state_city_region_csv,
    )

    end = EmptyOperator(task_id="end")

    start >> create_aqi_database >> create_table_aqi_rawdata >> end
    start >> get_state_data >> get_city_data >> generate_state_city_region_csv >> end
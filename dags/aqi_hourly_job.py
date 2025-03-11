import logging
import os
import pandas as pd
import numpy as np
from dags.aqi_class.AirQualityDatabase import AirQualityDatabase
from dags.aqi_class.ApiServices import ApiServices
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
master_file = "transform_state_city_region.csv"

file_path = os.path.join(dag_file_path, master_file)

apis = ApiServices(api_keys)
cms = CommonServices()
aqi_db = AirQualityDatabase(conn_id, api_url, api_keys, dag_file_path)

def _check_master_data():
    file_path = os.path.join(dag_file_path, master_file)
    if not cms.check_file_exists(file_path):
        raise FileNotFoundError(f"❌ ไม่พบไฟล์ {master_file}")
    else:
        logging.info(f"🆗 พบไฟล์ที่ {file_path}")

    df = pd.read_csv(file_path)
    quality_report = cms.check_data_quality(df)

    if quality_report["Missing Values"].sum() > 0 or quality_report["Outliers"].sum() > 0:
        logging.warning("⚠️ พบปัญหาในข้อมูล ตรวจสอบ Data Quality Report")

    logging.info(quality_report)

def _get_hourly_data():
    df = pd.read_csv(file_path)

    for _, row in df.iterrows():
        state, city, region = row["state"], row["city"], row["region"]

        # ✅ ดึงข้อมูลจาก API
        params = {
            "city": city,
            "state": state,
            "country": "thailand"
        }

        endpoint="v2/city"
        full_url = f"{api_url}{endpoint}"
        data = apis.fetch_api(url=full_url, params=params)

        # ✅ บันทึกลง Database
        aqi_db.insert_hourly_job((state, city, region), data)
        logging.info(f"✅ บันทึก AQI สำเร็จ: {city}, {state}, {region}")

with DAG(
    "aqi_hourly_job",
    schedule=None,
    start_date=timezone.datetime(2025, 3, 8),
    max_active_runs=1,  # ✅ จำกัดให้รันได้ครั้งละ 1 Task
    concurrency=1,      # ✅ จำกัดให้มี 1 Task ที่รัน API พร้อมกัน
    tags=["capstone","database"]
):
    start = EmptyOperator(task_id="start")

    check_master_data = PythonOperator(
        task_id="check_master_data",
        python_callable=_check_master_data,
    )

    get_hourly_data = PythonOperator(
        task_id="get_hourly_data",
        python_callable=_get_hourly_data,
    )

    end = EmptyOperator(task_id="end")

    start >> check_master_data >> get_hourly_data >> end
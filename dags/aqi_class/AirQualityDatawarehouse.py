import csv
import json
import os

from dags.aqi_class.CommonServices import CommonServices
from airflow.providers.postgres.hooks.postgres import PostgresHook

class AirQualityDatawarehouse:
    """Class สำหรับจัดการ Database, API และการสร้างไฟล์ JSON"""

    def __init__(self, conn_id: str, api_url: str, api_key: str, dag_file_path: str):
        self.conn_id = conn_id
        self.api_url = api_url
        self.api_key = api_key
        self.dag_file_path = dag_file_path
        self.cms = CommonServices()

    def create_aqi_datawarehouse(self):
        self.cms.check_conn_string()
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        connection = pg_hook.get_conn()
        connection.set_isolation_level(0)
        cursor = connection.cursor()

        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'aqi_datawarehouse';")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute("CREATE DATABASE aqi_datawarehouse;")
            connection.commit()
        
        connection.close()
        print("✅ Database 'aqi_datawarehouse' is ready!")
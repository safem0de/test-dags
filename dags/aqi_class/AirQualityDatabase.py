import json
import os
import requests
import time
from airflow.providers.postgres.hooks.postgres import PostgresHook


class AirQualityDatabase:
    """Class สำหรับจัดการ Database, API และการสร้างไฟล์ JSON"""

    def __init__(self, conn_id: str, api_url: str, api_key: str, dag_file_path: str):
        self.conn_id = conn_id
        self.api_url = api_url
        self.api_key = api_key
        self.dag_file_path = dag_file_path
        self.last_request_time = 0

        print(f"API Url: {self.api_url}")
        print(f"API Key: {self.api_key[:3]}******{self.api_key[-3:]}")


    # ✅ ตรวจสอบ Connection ของ Database
    def check_conn_string(self):
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
            connection = pg_hook.get_conn()
            cursor = connection.cursor()
            cursor.execute("SELECT 1;")
            connection.close()
            print(f"✅ Connection {self.conn_id} is valid!")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {str(e)}")
            return False


    # ✅ Execute SQL Query
    def execute_sql(self, database_name: str, sql_statement: str):
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.conn_id, database=database_name)
            connection = pg_hook.get_conn()
            cursor = connection.cursor()
            cursor.execute(sql_statement)
            connection.commit()
            cursor.close()
            connection.close()
            print(f"✅ SQL executed successfully on database: {database_name}")
        except Exception as e:
            print(f"❌ Error executing SQL on {database_name}: {str(e)}")


    # ✅ สร้างไฟล์ JSON ถ้ายังไม่มี
    def create_file_if_not_exist(self, filename: str, data: dict):
        file_path = os.path.join(self.dag_file_path, filename)
        
        if not isinstance(data, dict):  # ตรวจสอบว่า data เป็น dict
            print("❌ Error: Provided data is not a valid JSON dictionary.")
            return

        if not os.path.exists(file_path):
            with open(file_path, 'w') as file:
                json.dump(data, file, indent=4)
            print(f"✅ File created: {file_path}")
        else:
            print(f"⚠️ File already exists: {file_path}")


    def fetch_api(self, endpoint: str, rate_limit : int = 5, params: dict = None):
        """Fetch AQI data from API with dynamic parameters"""
        if not isinstance(rate_limit, int):
            raise ValueError(f"❌ rate_limit ต้องเป็น int แต่ได้รับ {type(rate_limit)}")

        request_interval = 60 / rate_limit  # เช่น 5 calls/min → รอ 12 วินาที/call
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        # ✅ ถ้ายังไม่ถึงเวลาที่กำหนด ให้รอ
        if time_since_last_request < request_interval:
            wait_time = request_interval - time_since_last_request
            print(f"⏳ Waiting {wait_time:.2f} seconds before next API call...")
            time.sleep(wait_time)

        url = f"{self.api_url}{endpoint}"
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ API Request failed: {e}")
            return None


    # ✅ สร้างฐานข้อมูล AQI ถ้ายังไม่มี
    def create_aqi_database(self):
        self.check_conn_string()
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        connection = pg_hook.get_conn()
        connection.set_isolation_level(0)
        cursor = connection.cursor()

        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'aqi_database';")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute("CREATE DATABASE aqi_database;")
            connection.commit()
        
        connection.close()
        print("✅ Database 'aqi_database' is ready!")


    # ✅ สร้างตาราง location
    def create_aqi_table_location(self):
        sql = """
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
        self.execute_sql("aqi_database", sql)


    # ✅ สร้างตาราง aqi_data
    def create_aqi_table_aqi_data(self):
        sql = """
            CREATE TABLE IF NOT EXISTS aqi_data (
                aqi_id SERIAL PRIMARY KEY,
                location_id INT,
                timestamp TIMESTAMP NOT NULL,
                aqius INT NOT NULL, 
                mainus VARCHAR(10),
                aqicn INT,
                maincn VARCHAR(10),
                FOREIGN KEY (location_id) REFERENCES location(location_id) ON DELETE CASCADE
            );
        """
        self.execute_sql("aqi_database", sql)


    # ✅ สร้างตาราง weather_data
    def create_aqi_table_weather_data(self):
        sql = """
            CREATE TABLE IF NOT EXISTS weather_data (
                weather_id SERIAL PRIMARY KEY,
                location_id INT,
                timestamp TIMESTAMP NOT NULL,
                temperature DECIMAL(5,2),
                pressure INT,
                humidity INT,
                wind_speed DECIMAL(5,2),
                wind_direction INT,
                FOREIGN KEY (location_id) REFERENCES location(location_id) ON DELETE CASCADE
            );
        """
        self.execute_sql("aqi_database", sql)


    # ✅ ดึงข้อมูล state (จังหวัด) จาก API และเก็บเป็นไฟล์ JSON
    def get_state_data(self, filename="state_master.json"):
        file_path = os.path.join(self.dag_file_path, filename)

        # ถ้าไฟล์มีอยู่แล้ว ไม่ต้องดึงข้อมูล
        if os.path.exists(file_path):
            print(f"✅ File '{file_path}' already exists. Skipping API call.")
            return

        params = {
            "country": "thailand",
            "key": self.api_key
        }
        data = self.fetch_api(endpoint="v2/states", params=params)
        print(data)

        self.create_file_if_not_exist(filename, data)


    # ✅ ดึงข้อมูล city (อำเภอ) ตามจังหวัดจาก API
    def get_city_data(self, state_name):
        filename = f"city_master_{state_name}.json"
        file_path = os.path.join(self.dag_file_path, filename)

        # ถ้าไฟล์มีอยู่แล้ว ไม่ต้องดึงข้อมูล
        if os.path.exists(file_path):
            print(f"✅ File '{file_path}' already exists. Skipping API call.")
            return

        params = {
            "state": state_name,
            "country": "thailand",
            "key": self.api_key
        }
        data = self.fetch_api(endpoint="v2/cities", params=params)
        print(data)

        self.create_file_if_not_exist(filename, data)

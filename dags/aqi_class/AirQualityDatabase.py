import csv
import json
import os
from dags.aqi_class.ApiServices import ApiServices
from dags.aqi_class.CommonServices import CommonServices

class AirQualityDatabase:
    """Class สำหรับจัดการ Database, API และการสร้างไฟล์ JSON"""

    def __init__(self, conn_id: str, api_url: str, api_keys: list, dag_file_path: str):
        self.conn_id = conn_id
        self.api_url = api_url
        self.api_keys = api_keys
        self.dag_file_path = dag_file_path
        self.apis = ApiServices(self.api_keys)
        self.cms = CommonServices()


    # ✅ สร้างฐานข้อมูล AQI ถ้ายังไม่มี
    def create_aqi_database(self):
        self.database_name = "aqi_database"
        self.cms.create_database(self.conn_id, self.database_name)


    # # ✅ สร้างตาราง location
    # def create_aqi_table_location(self):
    #     print("🔰 Start create table location")
    #     sql = """
    #         CREATE TABLE IF NOT EXISTS location (
    #             location_id SERIAL PRIMARY KEY,
    #             city VARCHAR(255) NOT NULL,
    #             state VARCHAR(255) NOT NULL,
    #             country VARCHAR(50) DEFAULT 'Thailand',
    #             latitude DECIMAL(10, 6),
    #             longitude DECIMAL(10, 6),
    #             region VARCHAR(255) NOT NULL,
    #             UNIQUE (city, state, country)
    #         );
    #     """
    #     self.cms.execute_sql(
    #         conn_id=self.conn_id, 
    #         database_name="aqi_database", 
    #         sql_statement=sql
    #         )


    # ✅ สร้างตาราง aqi_data
    def create_table_aqi_rawdata(self):
        print("🔰 Start create table air_quality_raw")
        sql = """
            CREATE TABLE air_quality_raw (
                aqi_id SERIAL PRIMARY KEY,
                city VARCHAR(255) NOT NULL,
                state VARCHAR(255) NOT NULL,
                region VARCHAR(255) NOT NULL,
                country VARCHAR(50) DEFAULT 'Thailand',
                latitude DECIMAL(10,6),
                longitude DECIMAL(10,6),
                timestamp TIMESTAMP NOT NULL,
                aqius INT NOT NULL,
                mainus VARCHAR(10),
                aqicn INT,
                maincn VARCHAR(10),
                temperature DECIMAL(5,2),
                pressure INT,
                humidity INT,
                wind_speed DECIMAL(5,2),
                wind_direction INT
            );
        """
        self.cms.execute_sql(
            conn_id=self.conn_id, 
            database_name="aqi_database", 
            sql_statement=sql
            )


    # ✅ ดึงข้อมูล state (จังหวัด) จาก API และเก็บเป็นไฟล์ JSON
    def get_state_data(self, filename="state_master.json"):
        file_path = os.path.join(self.dag_file_path, filename)

        # ถ้าไฟล์มีอยู่แล้ว ไม่ต้องดึงข้อมูล
        if os.path.exists(file_path):
            print(f"✅ File '{file_path}' already exists. Skipping API call.")
            return

        # ไม่ต้องใส่ API key เด๊ยว fetch api จัดให้
        params = {
            "country": "thailand",
        }

        endpoint="v2/states"
        full_url = f"{self.api_url}{endpoint}"

        data = self.apis.fetch_api(url=full_url, params=params)
        print(data)

        self.cms.create_file_if_not_exist(file_path=file_path, data=data)


    # ✅ ดึงข้อมูล city (อำเภอ) ตามจังหวัดจาก API
    def get_city_data(self, state_name):
        filename = f"city_master_{state_name}.json"
        file_path = os.path.join(self.dag_file_path, filename)

        # ถ้าไฟล์มีอยู่แล้ว ไม่ต้องดึงข้อมูล
        if os.path.exists(file_path):
            print(f"✅ File '{file_path}' already exists. Skipping API call.")
            return

        # ไม่ต้องใส่ API key เด๊ยว fetch api จัดให้
        params = {
            "state": state_name,
            "country": "thailand"
        }

        endpoint="v2/cities"
        full_url = f"{self.api_url}{endpoint}"

        data = self.apis.fetch_api(url=full_url, params=params)
        print(data)

        self.cms.create_file_if_not_exist(file_path=file_path, data=data)


    def generate_state_city_region_csv(self, dag_file_path, state_master, output_filename):
        """
        อ่านข้อมูลจาก `state_master.json` และ `city_master_xxx.json`
        แล้วบันทึกเป็นไฟล์ CSV ในรูปแบบ state, city
        """

        state_file = os.path.join(dag_file_path, state_master)
        output_file = os.path.join(dag_file_path, output_filename)

        # ✅ ตรวจสอบว่าไฟล์ state_master.json มีอยู่จริง
        if not os.path.exists(state_file):
            print(f"❌ Error: File '{state_file}' not found.")
            return

        # ✅ อ่านข้อมูล state_master.json
        with open(state_file, "r", encoding="utf-8") as f:
            state_data = json.load(f)

        # ✅ ตรวจสอบโครงสร้าง JSON
        if "data" not in state_data or not isinstance(state_data["data"], list):
            print(f"❌ Error: Invalid JSON structure in '{state_file}'.")
            return

        # ✅ เตรียมเก็บข้อมูล state, city
        state_city_list = []

        # ✅ วนลูปแต่ละจังหวัดเพื่ออ่านข้อมูลเมืองจาก city_master_xxx.json
        for state_entry in state_data["data"]:
            state_name = state_entry["state"]
            region_name = self.cms.map_region(state_name)  # ✅ หา region ตามจังหวัด
            city_file = os.path.join(dag_file_path, f"city_master_{state_name}.json")

            # ✅ ตรวจสอบว่าไฟล์ city_master_xxx.json มีอยู่จริง
            if os.path.exists(city_file):
                with open(city_file, "r", encoding="utf-8") as f:
                    city_data = json.load(f)

                # ✅ ตรวจสอบโครงสร้าง JSON
                if "data" in city_data and isinstance(city_data["data"], list):
                    cities = [city_entry["city"] for city_entry in city_data["data"]]
                else:
                    cities = []
            else:
                print(f"⚠️ Warning: File '{city_file}' not found. Skipping...")
                cities = []

            # ✅ เพิ่มข้อมูล state, city ลงใน list
            for city in cities:
                state_city_list.append([state_name, city, region_name])

        # ✅ เขียนข้อมูลลงไฟล์ CSV
        with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["state", "city", "region"])    # ✅ เขียน Header
            writer.writerows(state_city_list)               # ✅ เขียนข้อมูล

        print(f"✅ File saved: {output_file}")


    def insert_hourly_job(self, master_data, data):
        print("🔰 Start insert table air_quality_raw")

        try:
            state, city, region = master_data

            # ✅ Extract ข้อมูลจาก JSON อย่างปลอดภัย
            pollution = data.get("data", {}).get("current", {}).get("pollution", {})
            weather = data.get("data", {}).get("current", {}).get("weather", {})

            # ✅ ตรวจสอบค่าและกำหนดค่าเริ่มต้นถ้าหากไม่มีข้อมูล
            aqius = pollution.get("aqius", None)
            mainus = pollution.get("mainus", None)
            aqicn = pollution.get("aqicn", None)
            maincn = pollution.get("maincn", None)
            temperature = weather.get("tp", None)
            pressure = weather.get("pr", None)
            humidity = weather.get("hu", None)
            wind_speed = weather.get("ws", None)
            wind_direction = weather.get("wd", None)

            # ✅ SQL Query
            sql = """
                INSERT INTO air_quality_raw 
                (location_id, timestamp, aqius, mainus, aqicn, maincn, temperature, pressure, humidity, wind_speed, wind_direction)
                VALUES (
                    (SELECT COALESCE((SELECT location_id FROM location WHERE city = %s AND state = %s LIMIT 1), 0)),
                    NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s
                );
            """

            # ✅ กำหนดค่า Params
            params = (
                city, state, aqius, mainus, aqicn, maincn, 
                temperature, pressure, humidity, wind_speed, wind_direction
            )

            # ✅ บันทึกข้อมูลลงฐานข้อมูล
            self.cms.execute_sql(
                conn_id=self.conn_id, 
                database_name="aqi_database", 
                sql_statement=sql,
                parameters=params
            )

            print(f"✅ บันทึก AQI สำเร็จ: {city}, {state}, {region}")

        except Exception as e:
            print(f"❌ เกิดข้อผิดพลาดระหว่าง Insert AQI Data: {str(e)}")
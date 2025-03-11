import json
import os
import pandas as pd
import numpy as np

from airflow.providers.postgres.hooks.postgres import PostgresHook

class CommonServices:
    """Class สำหรับจัดการ Database, API และการสร้างไฟล์ JSON"""

    def __init__(self):
        pass


    def map_region(self, state:str):
        """
        คืนค่าภาค (region) ตามชื่อจังหวัด
        เหนือ	    North
        อีสาน	    Northeast
        กลาง	   Central
        ตะวันออก	East
        ตะวันตก     West
        ใต้         South
        """

        region_mapping = {
            "North": {'Chiang Mai', 'Chiang Rai', 'Lampang', 'Lamphun', 'Nan', 'Phayao', 'Phrae', 'Mae Hong Son', 'Sukhothai', 'Tak', 'Uttaradit'},
            "Northeast": {'Amnat Charoen', 'Buriram', 'Chaiyaphum', 'Changwat Bueng Kan', 'Changwat Ubon Ratchathani', 'Kalasin', 'Khon Kaen', 'Loei', 'Maha Sarakham', 'Mukdahan',
                    'Nakhon Phanom', 'Nakhon Ratchasima', 'Nong Bua Lamphu', 'Nong Khai', 'Roi Et', 'Sakon Nakhon', 
                    'Sisaket', 'Surin', 'Ubon Ratchathani', 'Udon Thani', 'Yasothon'},
            "Central": {'Ang Thong', 'Bangkok', 'Chai Nat', 'Kamphaeng Phet', 'Lopburi', 'Nakhon Nayok', 'Nakhon Pathom',
                    'Nakhon Sawan', 'Nonthaburi', 'Pathum Thani', 'Phetchabun', 'Phichit', 'Phitsanulok', 'Phra Nakhon Si Ayutthaya',
                    'Samut Prakan', 'Samut Sakhon', 'Samut Songkhram', 'Sara Buri', 'Sing Buri', 'Suphan Buri','Uthai Thani'},
            "East": {'Chachoengsao', 'Chanthaburi', 'Chon Buri', 'Prachin Buri', 'Rayong', 'Sa Kaeo', 'Trat'},
            "West": {'Kanchanaburi', 'Phetchaburi', 'Prachuap Khiri Khan', 'Ratchaburi', 'Tak'},
            "South": {'Chumphon', 'Krabi', 'Nakhon Si Thammarat', 'Narathiwat', 'Pattani', 'Phangnga', 'Phatthalung',
                    'Phuket', 'Ranong', 'Satun', 'Songkhla', 'Surat Thani', 'Trang', 'Yala'}
        }
        
        for region, states in region_mapping.items():
            if state in states:
                return region
        
        return "Unknown"  # Default ถ้าไม่พบข้อมูล
    

    def json_to_list(self, filename: str, parent_key: str, child_key: str) -> list:
        """
        Extracts a list of values from a JSON file based on the specified keys.

        :param filename: Path to the JSON file.
        :param parent_key: The key that contains the list of dictionaries.
        :param child_key: The key to extract values from each dictionary inside the parent_key list.
        :return: A list of extracted values.
        """
        try:
            # อ่าน JSON จากไฟล์
            with open(filename, "r", encoding="utf-8") as f:
                data = json.load(f)

            # ตรวจสอบว่า JSON เป็น dictionary และมี parent_key ที่ต้องการ
            if not isinstance(data, dict) or parent_key not in data:
                print(f"❌ Error: JSON file does not contain expected '{parent_key}' key.")
                return []

            # ตรวจสอบว่า parent_key มีข้อมูลเป็น list หรือไม่
            if not isinstance(data[parent_key], list):
                print(f"❌ Error: '{parent_key}' is not a list.")
                return []

            # ดึงค่าจาก child_key ในแต่ละ dictionary
            return [item.get(child_key, None) for item in data[parent_key]]

        except FileNotFoundError:
            print(f"❌ Error: File '{filename}' not found.")
            return []

        except json.JSONDecodeError:
            print(f"❌ Error: File '{filename}' is not a valid JSON file.")
            return []

    # ✅ ตรวจสอบ Connection ของ Database
    def check_conn_string(self, conn_id:str) -> bool:
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
    

    def create_database(self, conn_id:str, database_name:str):
        self.check_conn_string(conn_id)
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        connection = pg_hook.get_conn()
        connection.set_isolation_level(0)
        cursor = connection.cursor()

        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{database_name}';")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f"CREATE DATABASE {database_name};")
            connection.commit()
        
        connection.close()
        print(f"✅ Database '{database_name}' is ready!")


    # ✅ Execute SQL Query
    def execute_sql(self, conn_id:str, database_name:str, sql_statement:str, parameters: tuple = None) -> None:
        try:
            pg_hook = PostgresHook(postgres_conn_id=conn_id, database=database_name)
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            if parameters:
                cursor.execute(sql_statement, parameters)
            else:
                cursor.execute(sql_statement)

            connection.commit()
            print(f"✅ SQL executed successfully on database: {database_name}")
        except Exception as e:
            connection.rollback()
            print(f"❌ Error executing SQL on {database_name}: {str(e)}")
        finally:
            cursor.close()
            connection.close()


    # ✅ สร้างไฟล์ JSON ถ้ายังไม่มี
    def create_file_if_not_exist(self, file_path: str, data: dict) -> None:
        
        if not isinstance(data, dict):  # ตรวจสอบว่า data เป็น dict
            print("❌ Error: Provided data is not a valid JSON dictionary.")
            return

        if not os.path.exists(file_path):
            with open(file_path, 'w') as file:
                json.dump(data, file, indent=4)
            print(f"✅ File created: {file_path}")
        else:
            print(f"⚠️ File already exists: {file_path}")


    def check_file_exists(self, file_path) -> bool:
        if not os.path.exists(file_path):
            return False
        return True
    

    def check_data_quality(self, df:pd.DataFrame, expected_types:dict = None) -> pd.DataFrame:
        """
        ตรวจสอบคุณภาพข้อมูลใน DataFrame
        """
        print("🔍 ตรวจสอบคุณภาพข้อมูล DataFrame...\n")
        
        # ✅ ตรวจสอบ Missing Values (ค่า NaN / NULL)
        missing_values = df.isnull().sum()
        missing_percentage = (df.isnull().sum() / len(df)) * 100

        # ✅ ตรวจสอบค่าซ้ำซ้อน (Duplicated Rows)
        duplicate_count = pd.Series(df.duplicated().sum(), index=["Duplicate Count"])

        type_mismatch_series = pd.Series(dtype=int)
        if expected_types:
            type_mismatch = {
                col: df[col].apply(lambda x: not isinstance(x, expected_types.get(col, type(x)))).sum()
                for col in df.columns if col in expected_types
            }
        
            type_mismatch_series = pd.Series(type_mismatch).fillna(0).astype(int)

        # ✅ ตรวจสอบค่าที่อยู่นอกช่วง (Outliers) สำหรับค่าตัวเลข
        numerical_columns = df.select_dtypes(include=[np.number]).columns
        outliers = {}
        for col in numerical_columns:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            outliers[col] = df[(df[col] < lower_bound) | (df[col] > upper_bound)][col].count()

        outliers_series = pd.Series(outliers).fillna(0).astype(int)

        # ✅ รวมผลลัพธ์ทั้งหมดเป็น DataFrame
        quality_report = pd.DataFrame({
            "Missing Values": missing_values,
            "Missing %": missing_percentage,
            "Type Mismatch": type_mismatch_series,
            "Outliers": outliers_series
        }).fillna(0).astype(int)

        # ✅ เพิ่มข้อมูล Duplicates เป็นแถวใหม่
        quality_report = pd.concat([quality_report, duplicate_count.to_frame().T])
        print("✅ การตรวจสอบเสร็จสิ้น!")
        return quality_report
        
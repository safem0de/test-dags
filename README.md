# test-dags
```bash
test-dags/  <-- Root ของ Git repo
│── dags/   <-- โฟลเดอร์ที่เก็บ DAGs (จำเป็น)
│   ├── example_dag.py
│   ├── my_dag_1.py
│   ├── my_dag_2.py
│── requirements.txt  <-- (ถ้ามี) ใช้ติดตั้ง dependencies เพิ่มเติม
│── plugins/  <-- (ถ้ามี) ใช้สำหรับ custom plugins
│   ├── my_plugin.py
│── README.md  <-- (optional) ไฟล์อธิบาย repo
```

### COPY DATA จาก kube
```bash
# PowerShell
kubectl cp --retries=3 apache-airflow-test/airflow-bb554d448-6xfr4:/opt/airflow/dags ./airflow-dags -c airflow-webserver --no-preserve
```

### backup-file mode (skip some-pipeline)
```bash
cd dags
cp 4bba3da14b62f5785e7118c66744a2a9bcba73bf/airflow-dags/* .   
```

### ถ้ามีการ Import ข้อมูลใหม่
```bash
SELECT setval(pg_get_serial_sequence('air_quality_raw', 'aqi_id'), (SELECT MAX(aqi_id) FROM air_quality_raw));
```
หลังจากมีการนำเข้าข้อมูลด้วย COPY, INSERT ที่ระบุ aqi_id เอง, หรือ restore ข้อมูลจาก backup — sequence อาจจะล้าหลัง ทำให้มัน generate ค่าที่ชนกับของเดิม

### Simple Template
```bash
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

conn_id = "0_postgres_db"
cms = CommonServices()

def _test():
    pass

with DAG(
    "airquality_transform_data",
    schedule=None,
    start_date=timezone.datetime(2025, 3, 8),
    tags=["capstone","datawarehouse"]
):
    start = EmptyOperator(task_id="start")

    test = PythonOperator(
        task_id="test",
        python_callable=_test,
    )

    end = EmptyOperator(task_id="end")

    start >> test >>end
```
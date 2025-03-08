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
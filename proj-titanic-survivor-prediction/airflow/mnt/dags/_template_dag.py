from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
# all operator in learn---
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator # ไว้ทำ การดึง database เพื่อทำ bigqueryML 
) # ใน dags/breakfast_transaction_pipeline.py
# ------
# not learn yet
# load_from_gcs_to_bigquery
# https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/transfer/gcs_to_bigquery.html

# airflow hook? เพื่อความสะดวกในการ credential และการทำ secure สรุปคือ ถ้าใช้เชื่อมต่อกับ external ก้คือใช้ airflow connection จะ secure กว่า

from airflow.models import Variable
project_id = Variable.get("pim_gcp_project_id") # เช่น

with DAG(
    dag_id = "name_file",
    start_date=timezone.datetime(2024, 1, 22), 
    schedule="30 11 * * *", # ใช้ crontab linux
    tags=["breaksfast", "PIM"],
    
): #ใส่ : คือสิ่งเหล่านี้จะอยู่ใน context dag
    my_first_task = EmptyOperator(task_id="my_first_task")
    my_second_task = EmptyOperator(task_id="my_second_task")

# vs with default_args

# ## ให้ใช้ args ง่ายๆ เปลั้ยนที่เดียว
# default_args = {
#     "start_date": timezone.datetime(2024, 2, 25),
#     "owner": "Tor Thanawat",
# }

# # ถ้าเจอ with (มันคือ context manager) ข้างในก็จะใช้ตามชื่อ เช่น DAG ก็ไว้ทำ DAG 
# with DAG(
#     "titanic_from_mysql_to_bigquery_pipeline",
#     default_args=default_args,
#     schedule=None,
#     tags=["titanic", "mysql", "bigquery"],
# ):

my_first_task >> my_second_task # ไปดูหน้า graph  มันจะ link  กัน
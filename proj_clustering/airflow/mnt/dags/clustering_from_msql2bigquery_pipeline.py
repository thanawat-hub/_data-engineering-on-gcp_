from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow.providers.mysql.hooks.mysql import MySqlHook

import pandas as pd
import json 
from datetime import datetime

# airflow ติดตั้งพวก google services มาให้แล้ว ซึ่งก่อนหน้านี้ก็เหมือนที่เราต้องมา poetry add {lib}
from google.cloud import bigquery, storage
from google.oauth2 import service_account

from airflow.models import Variable

def _extract_from_mysql():
    hook = MySqlHook(mysql_conn_id="pim-clustering_mysql_conn") # บอกด้วยว่าต่อไปที่ใคร  #สร้างที่ airflow conn
    # hook.bulk_dump(table="titanic", tmp_file="/opt/airflow/dags/titanic_dump.tsv")
    # ถ้า dump มาแล้ว เราต้องการเห็นไฟล์ ซึ่งจะเห็นได้จากที่ airflow/mnt/  
    # เพราะจารย์ใช้ docker map path "/opt/airflow" # มาที่ airflow/mnt/ แล้ว

    # ใช้วิธีการจาก pd แทนการ bulk_dump เพราะติด permission
    conn = hook.get_conn() # ใช้ชื่อ ให้ตรงกับที่ save ตอน import.py ใน fn นี้ "df.to_sql"
    df = pd.read_sql("select * from clean_clustering", con=conn)
    # print(df.head())
    local_file_name = "clean_clustering.csv" # เข้าใจว่า ดึงจาก sql ที่เรา import.py เข้าไป มาใน local ของ container นี้
    df.to_csv(f"/opt/airflow/dags/{local_file_name}", index=False) #dumpfile, header=None  # ควรมี header ใน GCS

def _load_to_gcs():

    # BUSINESS_DOMAIN = "netflix" # ชื่อะไร? ปรากฏที่ไหน น่าจะใน buckets
    # location = "us-central1" # ใช้ให้ถูก ดูว่าที่สร้างอยู่ regions ไหน
    # project_id = "careful-voyage-410506" # ต้องเปลี่ยน แต่ถ้าใช้ Varible airflow ก็ไม่ต้องมา hard code แต่ละที่
    # bucket_name = tor1412
    BUSINESS_DOMAIN = Variable.get("BUCKETS_DOMAIN_NAME") # ตั้งว่าอะไรก็ได้ จะไปเป็นชื่อสุดท้ายใน table ของ bigquery
    project_id = Variable.get("pim_gcp_project_id") #
    location = Variable.get("location_pim_gcp_project") #
    bucket_name = Variable.get("bucket_name") #
    # ดูจากที่กำหนดใน GCS

    local_file_name = "clean_clustering.csv" #

    service_account_info_gcs = Variable.get(
        "pim_keyfile_load_to_gcs_secret",
        deserialize_json=True,
    )
    print(type(service_account_info_gcs)) 

    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    # Prepare and Load Credentials to Connect to GCP Services 
    # เราใช้จาก path container ก็ต้องใช้ opt
    keyfile_gcs = f"/opt/airflow/dags/pim-clustering-load-to-gcs-410506-a515b5ff892c.json" # ใช้ path ของ container ก็คือ /opt/airflow/ add both
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    # Load data from Local to GCS
    storage_client = storage.Client(
        project=project_id,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    file_path = f"/opt/airflow/dags/{local_file_name}"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{local_file_name}" # เปลี่ยนทุกครั้งนอกจาก pass parameter name csv มา

    # YOUR CODE HERE TO LOAD DATA TO GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

def _load_from_gcs_to_bigquery():
    
    BUSINESS_DOMAIN = Variable.get("BUCKETS_DOMAIN_NAME") # ตั้งว่าอะไรก็ได้ จะไปเป็นชื่อสุดท้ายใน table ของ bigquery
    project_id = Variable.get("pim_gcp_project_id")
    location = Variable.get("location_pim_gcp_project")
    bucket_name = Variable.get("bucket_name")  

    local_file_name = "clean_clustering.csv"#
    
    destination_blob_name = f"{BUSINESS_DOMAIN}/{local_file_name}"

    keyfile_bigquery = "/opt/airflow/dags/pim-clustering-load-from-gcs-to-bigquery-410506-3ae98eea6a59.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    # # Load data from GCS to BigQuery
    bigquery_client = bigquery.Client(
        project=project_id,
        credentials=credentials_bigquery,
        location=location,
    )
    table_id = f"{project_id}.{bucket_name}.{BUSINESS_DOMAIN}" # name table . in bigqury ที่ต้องมี table เพื่อเขียน bigquery ml ได้
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1, # ถ้าใช้ csv ถึงจะใช้อันนี้ได้
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format= bigquery.SourceFormat.CSV,
        autodetect=True,
    )
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()


## ให้ใช้ args ง่ายๆ เปลั้ยนที่เดียว
default_args = {
    # "start_date": timezone.datetime(2024, 3, 9),#/yyyy/dd/mm
    "start_date":datetime(2024, 3, 9),
    "schedule":"@daily",
    "owner": "Tor Thanawat",
}

# ถ้าเจอ with (มันคือ context manager) ข้างในก็จะใช้ตามชื่อ เช่น DAG ก็ไว้ทำ DAG 
with DAG(
    "clustering_from_mysql_to_bigquery_pipeline",
    default_args=default_args,
    schedule=None,
    tags=["customer_segment", "mysql", "bigquery"],
):

    # extract_from_mysql = EmptyOperator(task_id="extract_from_mysql") # รันได้แต่ไม่เจอ error เพราะว่า เรียกใช้ผิด Opeartor 
    extract_from_mysql = PythonOperator(
        task_id="extract_from_mysql",
        python_callable=_extract_from_mysql,
        ) 

    load_to_gcs = PythonOperator(
        task_id="load_to_gcs",
        python_callable=_load_to_gcs
        )

    load_from_gcs_to_bigquery = PythonOperator(
        task_id="load_from_gcs_to_bigquery",
        python_callable=_load_from_gcs_to_bigquery,
        # op_kwargs={
        #     "dags_folder": "{{var.value.dags_folder}}"
        # },
    )

    extract_from_mysql >> load_to_gcs >> load_from_gcs_to_bigquery# ไปดูหน้า graph  มันจะ link  กัน
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

def _load_from_gcs_to_bigquery():
    
    BUSINESS_DOMAIN = Variable.get("BUCKETS_DOMAIN_NAME") # ตั้งว่าอะไรก็ได้ จะไปเป็นชื่อสุดท้ายใน table ของ bigquery
    project_id = Variable.get("pim_gcp_project_id")
    location = Variable.get("location_pim_gcp_project")
    bucket_name = Variable.get("bucket_name")  

    local_file_name = "clean_clustering.csv"#
    
    destination_blob_name = f"{BUSINESS_DOMAIN}/{local_file_name}"

    keyfile_bigquery = "/opt/airflow/dags/mypim-410508-32ab7ef6997f.json"
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
        f"gs://{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()


## ให้ใช้ args ง่ายๆ เปลั้ยนที่เดียว
default_args = {
    # "start_date": timezone.datetime(2024, 3, 9),#/yyyy/dd/mm
    "start_date":datetime(2024, 4, 15),
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

    load_from_gcs_to_bigquery = PythonOperator(
        task_id="load_from_gcs_to_bigquery",
        python_callable=_load_from_gcs_to_bigquery,
        # op_kwargs={
        #     "dags_folder": "{{var.value.dags_folder}}"
        # },
    )

    
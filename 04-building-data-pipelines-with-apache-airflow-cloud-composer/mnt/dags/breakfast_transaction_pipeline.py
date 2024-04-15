from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

import json

# airflow ติดตั้งพวก google services มาให้แล้ว ซึ่งก่อนหน้านี้ก็เหมือนที่เราต้องมา poetry add {lib}
from google.cloud import bigquery, storage
from google.oauth2 import service_account

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
)



project_id = "careful-voyage-410506" # จาก GCS my first project
bucket_name = "breakfast-tor"


def _load_transactions_to_gcs(dags_folder=""):
    DATA_FOLDER = "data"
    BUSINESS_DOMAIN = "breakfast"
    location = "asia-southeast1"

    # Prepare and Load Credentials to Connect to GCP Services
    keyfile_gcs = f"{dags_folder}/careful-voyage-410506-c17a9fab3cf0.json" # ใช้ path ของ container ก็คือ /opt/airflow/ add both
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

    data = "transactions"
    # file_path = f"{DATA_FOLDER}/{data}.csv"
    file_path = f"{dags_folder}/datasets/breakfast_transactions.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"

    # YOUR CODE HERE TO LOAD DATA TO GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

    return destination_blob_name
###

def _load_transactions_from_gcs_to_bigquery(dags_folder="",**context):
    DATA_FOLDER = "data"
    BUSINESS_DOMAIN = "breakfast"
    location = "asia-southeast1"
    
    data = "transactions"
    # destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv" # change this to below เพราะถ้าจะแก้ชื่อ hardcode ทุกที ก็ไม่ใช่เรื่อง 
    destination_blob_name = context["ti"].xcom_pull(
            task_ids="load_transactions_to_gcs", key="return_value"
        )

    keyfile_bigquery = f"{dags_folder}/careful-voyage-410506-c17a9fab3cf0.json" # ใช้ path ของ container ก็คือ /opt/airflow/ add both
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
    table_id = f"{project_id}.breakfast.{data}"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()
###

with DAG(
    dag_id = "breakfast_transactions_pipeline",
    start_date=timezone.datetime(2024, 1, 22),
    schedule="30 11 * * *", 
    tags=["breaksfast", "PIM", "transactions"], 
        
): #ใส่ : คือสิ่งเหล่านี้จะอยู่ใน context dag


    start = EmptyOperator(task_id="start")

    load_transactions_to_gcs = PythonOperator(
        task_id="load_transactions_to_gcs",
        python_callable=_load_transactions_to_gcs,
        op_kwargs={
            "dags_folder": "{{var.value.dags_folder}}"
        },
    )

    load_transactions_from_gcs_to_bigquery = PythonOperator(
        task_id="load_transactions_from_gcs_to_bigquery",
        python_callable=_load_transactions_from_gcs_to_bigquery,
        op_kwargs={
            "dags_folder": "{{var.value.dags_folder}}"
        },
    )


    # มีส่วนนี้ เพราะใน miro ที่ว่าต้องมีตัวมากั้น โดยใช้ view ตอนเปลี่ยนค่าแล้วส่งไป dashboard ที่ looker หาไม่เจอ
    # https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_modules/tests/system/providers/google/cloud/bigquery/example_bigquery_tables.html
    # doc ใน [] คือ optional
    create_view = BigQueryCreateEmptyTableOperator(
        task_id="create_view",
        dataset_id="breakfast",
        table_id="transformed_transactions",
        gcp_conn_id="my_gcp_connection",
        view={
            "query": """
                select
                    extract(day from WEEK_END_DATE) as day
                    , extract(month from WEEK_END_DATE) as month
                    , extract(year from WEEK_END_DATE) as year
                    , WEEK_END_DATE as week_end_date
                    , STORE_NUM as store_num
                    , UPC as upc
                    , UNITS as units
                    , VISITS as visits
                    , BASE_PRICE as base_price
                from
                    `breakfast.transactions`
            """,
            "useLegacySql": False,
        },
    )

    end = EmptyOperator(task_id="end")

start >> load_transactions_to_gcs >> load_transactions_from_gcs_to_bigquery >> create_view >> end # 
from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id = "deploy",
    start_date=timezone.datetime(2024, 1, 22), 
    schedule=None,
    tags=["Google_Analytics", "PIM", "ML_training"]
    
): #ใส่ : คือสิ่งเหล่านี้จะอยู่ใน context dag
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    train_model = BigQueryExecuteQueryOperator(
    task_id="train_model",
        sql="""
            CREATE OR REPLACE MODEL `gdg_cloud_devfest_bkk_2020.purchase_prediction_model_{{ ds }}`
            OPTIONS(model_type='logistic_reg') AS
            SELECT
                IF(totals.transactions IS NULL, 0, 1) AS label,
                IFNULL(device.operatingSystem, "") AS os,
                device.isMobile AS is_mobile,
                IFNULL(geoNetwork.country, "") AS country,
                IFNULL(totals.pageviews, 0) AS pageviews
            FROM
                `bigquery-public-data.google_analytics_sample.ga_sessions_*`
            WHERE
                _TABLE_SUFFIX BETWEEN '20160801' AND '20170630'
        """,
        gcp_conn_id="my_gcp_connection",
    )

start >> train_model >> end # ไปดูหน้า graph  มันจะ link  กัน
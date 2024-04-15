from airflow import DAG
from airflow.utils import timezone
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator) # กรอก conn


default_args = {
    "start_date": timezone.datetime(2024, 4, 12), #yyyy-mm-dd
    "owner": "Tor Thanawat",
}

with DAG(
    "customer_cluster_modeling",
    default_args=default_args,
    schedule="@daily",
    tags=["clustering", "bigquery"],
):
    # เลือก k = 4 เพราะ ใช้ elbow แล้ว พบว่า จากข้อมูลนี้ k=4 เป็นจุดตัดที่อยุ่ต่ำสุด ระหว่าง distance กับจำนวน k 
    train_model_1 = BigQueryExecuteQueryOperator(
    task_id="train_model",
        sql="""
            CREATE OR REPLACE MODEL `mypim-410508.test1.testModelK4ClusteringaAddFeatureEng`
            OPTIONS (
                MODEL_TYPE = 'KMEANS',
                # Set the desired number of clusters here (e.g., 4)
                NUM_CLUSTERS = 4,
                # Use the recommended K-means++ initialization for better convergence
                KMEANS_INIT_METHOD = 'KMEANS++',
                # Standardize features for better distance calculations (assuming numerical data)
                STANDARDIZE_FEATURES = TRUE
            ) AS
                SELECT
                Education,
                Income,
                Kidhome,
                Teenhome,
                Recency,
                Wines,
                Fruits,
                Meat,
                Fish,
                Sweets,
                Gold,
                NumDealsPurchases,
                NumWebPurchases,
                NumCatalogPurchases,
                NumStorePurchases,
                NumWebVisitsMonth,
                Age,
                Spent,
                Living_With,
                Children,
                Family_Size,
                Is_Parent
            FROM
                `mypim-410508.test1.test_cluster`
    """,
        gcp_conn_id="my_gcp_connection", # using type goolge cloud และดูว่ามีสิทธิ์ create model ไหม ## ต้องแก้ใน airflow connection
        use_legacy_sql=False,
    )

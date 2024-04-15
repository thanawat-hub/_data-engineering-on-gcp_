from airflow import DAG
from airflow.utils import timezone
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator) # กรอก conn


default_args = {
    "start_date": timezone.datetime(2024, 2, 25),
    "owner": "Tor Thanawat",
}

with DAG(
    "titanic_survivor_predictor_modeling",
    default_args=default_args,
    schedule="@daily",
    tags=["titanic", "bigquery"],
):

    train_model_1 = BigQueryExecuteQueryOperator(
    task_id="train_model",
        sql="""
            create or replace model `tor1412.suvivor_predictor_just_sex_feature`
            options(model_type='logistic_reg') as 
            select
                sex,
                survived as label
            from `tor1412.titanic`
    """,
        gcp_conn_id="my_gcp_connection", # using type goolge cloud และดูว่ามีสิทธิ์ create model ไหม
        use_legacy_sql=False,
    )

    train_model_2 = BigQueryExecuteQueryOperator(
        task_id="train_model_2",
        sql="""
            create or replace model `tor1412.suvivor_predictor_add_age_feature`
            options(model_type='logistic_reg') as
            select
                Sex,
                Age,
                Survived as label
            from `tor1412.titanic`
        """,
        gcp_conn_id="my_gcp_connection",
        use_legacy_sql=False,
    )


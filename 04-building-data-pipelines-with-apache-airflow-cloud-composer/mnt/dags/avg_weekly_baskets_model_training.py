from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id = "avg_weekly_baskets_model_training",
    start_date=timezone.datetime(2024, 1, 22), 
    schedule=None,
    tags=["Google_Analytics", "PIM", "ML_training_baskets"]
    
): #ใส่ : คือสิ่งเหล่านี้จะอยู่ใน context dag
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")


    train_model = BigQueryExecuteQueryOperator(
    task_id="train_model",
        sql="""
        create or replace model `breakfast.avg_weekly_baskets_model` 
        options(model_type='LINEAR_REG') as
        select
        AVG_WEEKLY_BASKETS as label
        , CATEGORY as category
        , STORE_NAME as store_name
        , MANUFACTURER as manufacturer
        , ADDRESS_CITY_NAME as address_city_name
        from `breakfast.one_big_table_as_view`
        """,
        gcp_conn_id="my_gcp_connection",
        use_legacy_sql=False,
    )

    get_model_training_statistics = BigQueryExecuteQueryOperator(
    task_id="get_model_training_statistics",
    sql="""
        CREATE OR REPLACE TABLE
            `breakfast.avg_weekly_baskets_model_training_statistics_{{ ds }}` AS
        SELECT
            *
        FROM
            ML.TRAINING_INFO(MODEL `breakfast.avg_weekly_baskets_model`)
        ORDER BY
            iteration DESC
    """,
    allow_large_results=True,
    use_legacy_sql=False,
    gcp_conn_id="my_gcp_connection",
    )

    evaluate_model = BigQueryExecuteQueryOperator(
        task_id="evaluate_model",
        sql="""
        create or replace model `breakfast.avg_weekly_baskets_model` 
        options(model_type='LINEAR_REG') as
        select
        AVG_WEEKLY_BASKETS as label
        , CATEGORY as category
        , STORE_NAME as store_name
        , MANUFACTURER as manufacturer
        , ADDRESS_CITY_NAME as address_city_name
        from `breakfast.one_big_table_as_view`
        """,
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="my_gcp_connection",
    )
    
#    # for model classify 
    # compute_roc = BigQueryExecuteQueryOperator(
    #     task_id="compute_roc",
    #     sql="""
    #         CREATE OR REPLACE TABLE
    #             `breakfast.avg_weekly_baskets_model_roc_{{ ds }}` AS
    #         SELECT
    #             *
    #         FROM
    #             ML.ROC_CURVE(MODEL `breakfast.avg_weekly_baskets_model`)
    #     """,
    #     allow_large_results=True,
    #     use_legacy_sql=False,
    #     gcp_conn_id="my_gcp_connection",
    # )
    
#    # for model regresion


start >> train_model >> [get_model_training_statistics, evaluate_model] >> end
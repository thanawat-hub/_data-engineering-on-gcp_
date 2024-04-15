from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id = "ga_transaction_model_training",
    start_date=timezone.datetime(2024, 1, 22), 
    schedule=None,
    tags=["Google_Analytics", "PIM", "ML_training"]
    
): #ใส่ : คือสิ่งเหล่านี้จะอยู่ใน context dag
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")


    train_model = BigQueryExecuteQueryOperator(
    task_id="train_model",
        sql="""
            CREATE OR REPLACE MODEL `bqml.ga_transactional_model_{{ ds }}`
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
        use_legacy_sql=False,
    )

    get_model_training_statistics = BigQueryExecuteQueryOperator(
    task_id="get_model_training_statistics",
    sql="""
        CREATE OR REPLACE TABLE
            `bqml.ga_transactional_model_training_statistics_{{ ds }}` AS
        SELECT
            *
        FROM
            ML.TRAINING_INFO(MODEL `bqml.ga_transactional_model`)
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
            CREATE OR REPLACE TABLE
                `bqml.ga_transactional_model_evaluation_{{ ds }}` AS
            SELECT
                *
            FROM ML.EVALUATE(MODEL `bqml.ga_transactional_model`, (
                SELECT
                    IF(totals.transactions IS NULL, 0, 1) AS label,
                    IFNULL(device.operatingSystem, "") AS os,
                    device.isMobile AS is_mobile,
                    IFNULL(geoNetwork.country, "") AS country,
                    IFNULL(totals.pageviews, 0) AS pageviews
                FROM
                    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                WHERE
                    _TABLE_SUFFIX BETWEEN '20170701' AND '20170801'))
        """,
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="my_gcp_connection",
    )
    
    compute_roc = BigQueryExecuteQueryOperator(
        task_id="compute_roc",
        sql="""
            CREATE OR REPLACE TABLE
                `bqml.ga_transactional_model_roc_{{ ds }}` AS
            SELECT
                *
            FROM
                ML.ROC_CURVE(MODEL `bqml.ga_transactional_model`)
        """,
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="my_gcp_connection",
    )

start >> train_model >> [get_model_training_statistics, evaluate_model, compute_roc] >> end
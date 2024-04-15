from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id = "name_file",
    start_date=timezone.datetime(2024, 1, 22), 
    schedule="30 11 * * *", # ใช้ crontab linux
    tags=["breaksfast", "PIM"],
    
): #ใส่ : คือสิ่งเหล่านี้จะอยู่ใน context dag
    my_first_task = EmptyOperator(task_id="my_first_task")
    my_second_task = EmptyOperator(task_id="my_second_task")

my_first_task >> my_second_task # ไปดูหน้า graph  มันจะ link  กัน
from airflow import DAG

from airflow.utils import timezone

from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id = "my_first_dag",#dag id แนะนำให้ตั้งเป็นชื่อเดียวกับตัวชื่อไฟล์
    start_date=timezone.datetime(2024, 1, 22), # ใช้ datetime ของ python ได้ แต่มีเรื่องของ timezone ที่ต้องจัดการเอง แต่ airflow มีจัดการให้
    schedule="30 11 * * *", # ใช้แบบ crontab linux
    tags=["breaksfast", "PIM"], # ก็คือ tag ที่แสดงในหน้า UI airflow ใช้ filter
        
): #ใส่ : คือสิ่งเหล่านี้จะอยู่ใน context dag
    my_first_task = EmptyOperator(task_id="my_first_task")
    my_second_task = EmptyOperator(task_id="my_second_task")

my_first_task >> my_second_task # ไปดูหน้า graph  มันจะ link  กัน
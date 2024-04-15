from airflow import DAG

from airflow.utils import timezone

from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _print_python():
    print("_print_python")

with DAG(
    dag_id = "my_second_dag",
    start_date=timezone.datetime(2024, 1, 22),
    schedule="40 11 * * *",
        
):
    start = EmptyOperator(task_id="start")
    
    hello_world = BashOperator(
        task_id="hello_world",
        bash_command="echo 'hello_world'"

    )

    print_python = PythonOperator(
        task_id="_print_python",
        python_callable=_print_python
    )

    end = EmptyOperator(task_id="end")

# --- seq
    # normal chain
    # start >> hello_world >> print_python >> end
# ---

# --- parallel | การ design ขึ้นกับ worker ที่มีด้วย
    # define seperate dependecy 
    # start >> hello_world  >> end
    # start >> print_python >> end
# ---
    # ซึ่งการใช้ define seperate dependecy เหมือนกับshort-hand
    #short-hand 
    start >> [hello_world,print_python] >> end
from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import json
import requests

from airflow.models import Variable

# DAGS_FOLDER = "/opt/airflow/dags" # check / 
DAGS_FOLDER = Variable.get("dags_folder")

def _get_dog_image_url():
    url = "https://dog.ceo/api/breeds/image/random"
    response = requests.get(url)
    data = response.json()
    # print(data)

    with open(f"{DAGS_FOLDER}/dogs.json", "w") as f: # ของเดิม มันไปอยู้ใน default path ใน container อาจจะหาไม่เจอ ซึ่งมันควรไปลงใน folder dags
        json.dump(data,f)
        # ไม่สามารถ copy path จากด้านซ้ายมาได้ เพราะมันอยู่ใน container แล้ว โดย

with DAG(
    dag_id = "dog_api_pipeline",
    start_date=timezone.datetime(2024, 1, 22),
    schedule="*/30 * * * *",
    catchup=False
):
    start = EmptyOperator(task_id="start")
    

    _get_dog_image_url = PythonOperator(
        task_id="_get_dog_image_url",
        python_callable=_get_dog_image_url
    )

    # เดิมอาจใส่แบบนี้ แล้วต้อง hard code 
    # API_KEY='$2a$10$LiwZDQpo9LsUhSG3b48ck.3jUGeVaJ2CmFxAlMp03qa9EJ2kVits6' 
    # COLLECTION_ID='659a4d141f5677401f189ff8'
    
    # templated ใช้ {{}}
    load_to_jsonbin = BashOperator(
        task_id="load_to_jsonbin",
        bash_command=f"""
        API_KEY='{{ {{var.value.jsonbin_api_key}} }}'
        COLLECTION_ID='{{ {{var.value.jsonbin_dog_collection_id}} }}'

        curl -XPOST \
            -H "Content-type: application/json" \
            -H "X-Master-Key: $API_KEY" \
            -H "X-Collection-Id: $COLLECTION_ID" \
            -d @{{DAGS_FOLDER}}/dogs.json \
            "https://api.jsonbin.io/v3/b"
        """,
        # แก้ path ตรงนี้ด้วย เพราะเป็นการ load dogs.json 
        # แล้วถ้าเป็น bash_command  ใช้ """ คั่น 
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
    start >> [_get_dog_image_url] >> load_to_jsonbin >> end
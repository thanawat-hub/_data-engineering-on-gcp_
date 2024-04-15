import json
import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account

st.title("Titanic Survivor Predictor")

# data = st.slider("Test", 0, 5, 1)
# print(data)

sex_option = st.selectbox(
   "Sex",
   ("Male", "female"),
   index=None,
   placeholder="Select contact method...",
)
st.write('You selected:', sex_option)

if st.button("Predict"): # ถ้าปุ่มโดนกด ให้ทำเงื่อนไขด้านใน
    # read model
    # make prediction

    # เหมือนกับการไปสร้าง client ไป bigquery ปกติ คือปกติถ้าจะใช้ service ก็ต้องกำหนด client ที่มี credential เพื่อทำบางสิ่ง 
    # ที่เราจะทำคือ prediction ดด้วยการส่ง query เข้าไปใน service (อันนี้คือเหมือนยิง API เป็น query ไปได้ผลเป็น ouput มา)
    keyfile_bigquery = "airflow/mnt/dags/pim-titanic-load-from-gcs-to-bigquery-410506-3ae98eea6a59.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )
    ### ทำประมาณนี้เสมอ ถ้าทำอะไรกับ service gcp ก็มี client กับ credential ประมาณนี้ (เป็นท่าที่ใช้ API)
    ### หรืออีกวิธีคือ export model จาก biguqery มาแล้วเขียน tensorflow มารองรับ

    project_id = "careful-voyage-410506"
    bigquery_client = bigquery.Client(
        project=project_id,
        credentials=credentials_bigquery,
        location="us-central1",
    )
    query = f"""
        select * from ml.predict(model `tor1412.suvivor_predictor_just_sex_feature`, (
                select '{sex_option}' as Sex
            )
        )
    """
    df = bigquery_client.query(query).to_dataframe()
    print(df.head())

    survived = df["predicted_label"][0]
    if survived:
        result = "Survived"
    else:
        result = "Died.. ☠️"

    st.write(result)
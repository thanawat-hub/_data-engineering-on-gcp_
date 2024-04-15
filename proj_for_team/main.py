import json
import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account

import time 
import pandas as pd

import numpy as np

def call_api(education_numeric, income, kidhome, teenhome, recency, wines, fruits, meat, fish, sweets, gold, num_deals_purchases, num_web_purchases, num_catalog_purchases, num_store_purchases, num_web_visits_month, age, spent,living_With, children, family_Size, is_Parent):
    # เหมือนกับการไปสร้าง client ไป bigquery ปกติ คือปกติถ้าจะใช้ service ก็ต้องกำหนด client ที่มี credential เพื่อทำบางสิ่ง 
    # ที่เราจะทำคือ prediction ดด้วยการส่ง query เข้าไปใน service (อันนี้คือเหมือนยิง API เป็น query ไปได้ผลเป็น ouput มา)
    keyfile_bigquery = "airflow/mnt/dags/mypim-410508-ffec3538093f.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )
    ### ทำประมาณนี้เสมอ ถ้าทำอะไรกับ service gcp ก็มี client กับ credential ประมาณนี้ (เป็นท่าที่ใช้ API)
    ### หรืออีกวิธีคือ export model จาก biguqery มาแล้วเขียน tensorflow มารองรับ

    project_id = "mypim-410508"
    bigquery_client = bigquery.Client(
        project=project_id,
        credentials=credentials_bigquery,
        location="us-central1",
    )

    query = f"""
        SELECT * FROM ML.PREDICT(MODEL `mypim-410508.test1.testModelK4ClusteringaAddFeatureEng`,
                ( SELECT
                {education_numeric} as Education,
                {income} as Income,
                {kidhome} as Kidhome,
                {teenhome} as Teenhome,
                {recency} as Recency,
                {wines} as Wines,
                {fruits} as Fruits,
                {meat} as Meat,
                {fish} as Fish,
                {sweets} as Sweets,
                {gold} as Gold,
                {num_deals_purchases} as NumDealsPurchases,
                {num_web_purchases} as NumWebPurchases,
                {num_catalog_purchases} as NumCatalogPurchases,
                {num_store_purchases} as NumStorePurchases,
                {num_web_visits_month} as NumWebVisitsMonth,
                {age} as Age,
                {spent} as Spent,
                {living_With} as Living_With,
                {children} as Children,
                {family_Size} as Family_Size,
                {is_Parent} as Is_Parent
            )
        )
        """
    df = bigquery_client.query(query).to_dataframe()

    # st.write("Result table from bigquery")
    # st.write(df.head())

    return df

def main():
    
    # Sidebar for customer information
    st.sidebar.subheader("Customer Information")
    
    education_options_mapping = {
        "Undergraduate": 0,
        "Graduate": 1,
        "Postgraduate": 2
    }
    
    selected_education = st.sidebar.selectbox("Education", ["Undergraduate","Graduate","Postgraduate"], help="Customer's education level")
    education_numeric = education_options_mapping[selected_education]
    # education = st.sidebar.selectbox("Education", list(education_options.keys()), key="education" ,help="Customer's education level")
    # จัดกลุ่มจากเดิมที่เป็นหลายแบบให้เหลือ3 แบบ Postgraduate(จบเกินตรี), Graduate(จบตรี), Undergraduate(จบไม่ถึงตรี)

    age = st.sidebar.number_input("Age", value=55, help="Customer's Age")
    # year now 2024 ลบ ปีเกิด
    income = st.sidebar.number_input("Income", value=70000, help="Customer's yearly household income")
    # รายได้ครัวเรือนต่อปีของลูกค้า
    kidhome = st.sidebar.number_input("Kidhome", value=1, help="Number of children in customer's household")
    # จน เด็กในบ้าน
    teenhome = st.sidebar.number_input("Teenhome", value=1, help="Number of teenagers in customer's household")
    # จน วัยรุ่นในบ้าน
    
    living_With = st.sidebar.selectbox("Living_With", [0, 1], help="It is Marital status (Number 1 = Marital, Number 0 = Single ")
    # เดิมจะเป็นประมาณนี้ data["Living_With"]=data["Marital_Status"].replace({"Married":"Partner", "Together":"Partner", "Absurd":"Alone", "Widow":"Alone", "YOLO":"Alone", "Divorced":"Alone", "Single":"Alone",})
    is_Parent = st.sidebar.selectbox("Is_Parent", [0, 1], help="It is Parent status (if your is parent = 1, else = 0)")
    # เป็นพ่อแม่ คนหรือไม่

    children = int(kidhome) + int(teenhome)
    # st.write(children)
    # children = st.sidebar.number_input("Children", value=1, help="Number of Children in customer's household (kidhome + teenhome)")
    # (kidhome + teenhome)

    family_Size = int(children) + int(living_With)
    # st.write(family_Size)
    #family_Size = st.sidebar.number_input("Family_Size", value=1, help="Number of family in customer's household (Children + liveing_With)")
    # ซึ่งถ้าเป็น alone คือ = 1 และ partner คือ = 2 คน


    # Grouping related inputs into columns
    col1, col2, col3 = st.columns(3)
    
    with col1:
        with st.expander("Purchase History"):
            st.write("Customer's purchase history in last 2 years.")
            recency = st.number_input("Recency", value=6, help="Number of days since customer's last purchase")
            wines = st.number_input("Wines", value=78, help="Amount spent on wine in last 2 years")
            fruits = st.number_input("Fruits", value=10, help="Amount spent on fruits in last 2 years")
            meat = st.number_input("Meat", value=200, help="Amount spent on meat in last 2 years")
        
    with col2:
        with st.expander("Purchase History Behavior"):
            st.write("Customer's behavior purchase history in last 2 years.")
            fish = st.number_input("Fish", value=40, help="Amount spent on fish in last 2 years")
            sweets = st.number_input("Sweets", value=10, help="Amount spent on sweets in last 2 years")
            gold = st.number_input("Gold", value=3, help="Amount spent on gold in last 2 years")
        
    with col3:
        with st.expander("Purchase Activity"):
            # st.write("This section includes details about the customer's purchase activity.")
            num_deals_purchases = st.number_input("NumDealsPurchases", value=5, help="Number of purchases made with a discount")
            #ซื้อของอันที่ลดราคา กี่ครั้ง
            num_web_purchases = st.number_input("NumWebPurchases", value=9, help="Number of purchases made through the company’s website")
            #ซื้อผ่านหน้าเว็บเรากี่ครั้ง            
            num_catalog_purchases = st.number_input("NumCatalogPurchases", value=8, help="Number of purchases made using a catalogue")
            #เลือกซื้อสินค้าจาก catalog ที่บริษัทจัดทำขึ้นมาให้ และทำการสั่งซื้อสินค้าโดยตรงจากแคตตาล็อกนั้นๆ โดยไม่ต้องเข้าไปที่ร้านหรือใช้ช่องทางการซื้ออื่นๆ
            num_store_purchases = st.number_input("NumStorePurchases", value=7, help="Number of purchases made directly in stores")
            # ซื้อหน้าร้าน ไม่ผ่าน catalog
            num_web_visits_month = st.number_input("NumWebVisitsMonth", value=6, help="Number of visits to company’s website in the last month")
            # จน customer เข้า website ใน เดือนที่ผ่านมา
            spent = st.number_input("Spent", value=1150, help="Total amount spent in last 2 years")
            # จน เงินที่ใช้จ่าย = Monetary  จำนวนมูลค่ารวมของ Transaction ในช่วงเวลา 2 ปีที่ผ่านมา

            frequency = num_deals_purchases + num_web_purchases + num_catalog_purchases + num_store_purchases
            # Frequency = จำนวน Transaction ในช่วงเวลา 2 ปีที่ผ่านมา คือ ตัว Purchase Activity

    
    if st.button("Submit"):
        # st.write(education_numeric)
        # df = call_api(education_numeric, income, kidhome, teenhome, recency, wines, fruits, meat, fish, sweets, gold, num_deals_purchases, num_web_purchases, num_catalog_purchases, num_store_purchases, num_web_visits_month, age, spent, living_With, children, family_Size, is_Parent)

        # st.write("Result table from bigquery")
        # st.write(df['CENTROID_ID'].head())
        # st.write(df['NEAREST_CENTROIDS_DISTANCE'].head())
        
        #--
# # Read the JSON data into a DataFrame
#         data = {
#         "NEAREST_CENTROIDS_DISTANCE": [{
#             "CENTROID_ID": "4",
#             "DISTANCE": "4.466042287568448"
#         }, {
#             "CENTROID_ID": "2",
#             "DISTANCE": "5.6914899863683353"
#         }, {
#             "CENTROID_ID": "1",
#             "DISTANCE": "6.836384349534316"
#         }, {
#             "CENTROID_ID": "3",
#             "DISTANCE": "7.0810320923270647"
#         }]
#         }

#         df = pd.DataFrame(data["NEAREST_CENTROIDS_DISTANCE"])

#         # Convert 'DISTANCE' column to float
#         df['DISTANCE'] = df['DISTANCE'].astype(float)

#         # Streamlit app
#         st.title('Display JSON Data in Streamlit')

#         st.write('Original DataFrame:')
#         st.write(df)

#         # Optionally, you can display the DataFrame as a table
#         st.write('Display DataFrame as Table:')
#         st.write(df.style.format({'DISTANCE': '{:.2f}'}))  # Format DISTANCE column to two decimal places        
        #--

        # data = {"Distance to the nearest centroid from k = 4 (from elbow) ": df['NEAREST_CENTROIDS_DISTANCE'].to_dict()}
        # st.write('For more detail -> ')
        # st.write(data)

        # group_segment = str(df['CENTROID_ID'][0]).strip()

        # st.subheader("Customer Segment is group: "+ group_segment)# " (from call model API on bigqurey)")

        group_segment = "3"
        st.write("Customer Segment is group: "+ group_segment +"(fix for test)")
         
        if group_segment == "4":
            st.subheader('Progress bar for do something after clustering customer')
    
            # Create a container for progress bar and text
            container = st.container()

            # Create a progress bar widget
            progress_bar = container.progress(0)

            # Create a text widget for displaying dots
            text = container.empty()

            # Function to update progress bar
            def update_progress(progress):
                progress_bar.progress(progress)

            # Function to update text
            def update_text(dot_count):
                text.text("somthing analysis " + "." * dot_count)

            dot_count = 0

            # Simulate a long-running process
            for i in range(100):
                time.sleep(0.05)  # Simulating some computation
                update_progress(i + 1)  # Update progress
                if dot_count < 3:
                    dot_count += 1
                else:
                    dot_count = 1
                update_text(dot_count)
            text.empty()
            # Write additional text in the same container after the loop finishes

            container.subheader("From ... Analysis suggestion for this customer group 4 is ... ")    

        # Define the values for "frequency", "spent", and "recency"
        monetary = num_deals_purchases + num_web_purchases + num_catalog_purchases + num_store_purchases # 
        # spent_value = spent
        # recency_value = recency

        # st.write("Spent:", spent)
        # st.write("Recency:", recency)
        # st.write("Monetary:", monetary)

        def normalize(value, min_val, max_val, ascending=True):
            if ascending:
                return ((value - min_val) / (max_val - min_val)) * 100
            else:
                return ((max_val - value) / (max_val - min_val)) * 100

        # # Example data
        # spent = 500      # Money spent
        # recency = 1     # Recency (number of days)
        # monetary = 3     # Monetary value (purchase frequency)

        # ต้องมากำหนดเพื่อทำ normalize เสมอ ถ้ามีค่าทะลุตรงนี้ ก้แตก
        # Define the minimum and maximum values for each variable
        min_spent, max_spent = 0, 10000   # Example minimum and maximum spent
        min_recency, max_recency = 0, 99 # Example minimum and maximum recency
        min_monetary, max_monetary = 0, 8235 # Example minimum and maximum monetary

        # Normalize each variable
        normalized_spent = normalize(spent, min_spent, max_spent)
        normalized_recency = normalize(recency, min_recency, max_recency, ascending=False)
        normalized_monetary = normalize(monetary, min_monetary, max_monetary)

        st.write("Normalized Spent:", normalized_spent)
        st.write("Normalized Recency:", normalized_recency)
        st.write("Normalized Monetary:", normalized_monetary)


        # # Define RFM score ranges and corresponding segments
        # segments = {
        #     (5, 5, 5): "Champion",
        #     (3, 5, 5): "Loyal Customer",
        #     (5, 1, 0): "New Customer",
        #     (3, 3, 3): "Need Attention",
        #     (1, 5, 5): "Can’t Lose Them",
        #     (1, 1, 1): "Lost"
        # }

        # # Map RFM score to segment
        # rfm_score = (normalized_recency, normalized_monetary, normalized_spent)
        # segment = segments.get(rfm_score, "Other")

        # Assuming you already have the normalized values
# normalized_recency, normalized_frequency, and normalized_monetary

        # https://www.geeksforgeeks.org/rfm-analysis-analysis-using-python/ 
        # Define the weights for each factor in RFM score calculation
        weight_recency = 0.15
        weight_frequency = 0.28 # ของเราคือชื่อว่า monetary
        weight_monetary = 0.57 # monetary ของเขาคือ spent

        # Calculate RFM score 
        RFM_Score = (weight_recency * normalized_recency) + \
                    (weight_frequency * normalized_monetary) + \
                    (weight_monetary * normalized_spent)

        # Scale RFM score to a range of 0 to 5
        RFM_Score *= 0.05

        # Round RFM score to two decimal places
        RFM_Score = round(RFM_Score, 2)

        st.write("RFM Score:", RFM_Score)

        def classify_customer(RFM_Score):
            if RFM_Score > 4.5:
                return "Top Customer"
            elif 4.5 >= RFM_Score > 4:
                return "High Value Customer"
            elif 4 >= RFM_Score > 3:
                return "Medium Value Customer"
            elif 3 >= RFM_Score > 1.6:
                return "Low-Value Customer"
            else:
                return "Lost Customer"

        
        customer_classification = classify_customer(RFM_Score)

        # Print the classification result
        st.write("Customer Classification:", customer_classification)


if __name__ == "__main__":
    main()

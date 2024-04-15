import json
import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account

import time 

def call_api(education_numeric, income, kidhome, teenhome, recency, wines, fruits, meat, fish, sweets, gold, num_deals_purchases, num_web_purchases, num_catalog_purchases, num_store_purchases, num_web_visits_month, age, spent,living_With, children, family_Size, is_Parent):
    # เหมือนกับการไปสร้าง client ไป bigquery ปกติ คือปกติถ้าจะใช้ service ก็ต้องกำหนด client ที่มี credential เพื่อทำบางสิ่ง 
    # ที่เราจะทำคือ prediction ดด้วยการส่ง query เข้าไปใน service (อันนี้คือเหมือนยิง API เป็น query ไปได้ผลเป็น ouput มา)
    keyfile_bigquery = "airflow/mnt/dags/pim-clustering-load-from-gcs-to-bigquery-410506-3ae98eea6a59.json"
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
        SELECT * FROM ML.PREDICT(MODEL `careful-voyage-410506.customerdata1412.modelclustering`,
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
    group_segment = str(df['CENTROID_ID'][0]).strip()

    # if  group_segment == "1":
    #     st.write("Customer group is:"+group_segment)
        
    # if  group_segment == "2":
    #     st.write("Customer group is:"+group_segment)
    # if  group_segment == "3":
    #     st.write("Customer group is: "+group_segment)

    # if  group_segment == "4":
    #     st.write("Customer group is: "+group_segment)

    return group_segment

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
            # จน เงินที่ใช้จ่าย

    if st.button("Submit"):
        # st.write(education_numeric)
        # group_segment = call_api(education_numeric, income, kidhome, teenhome, recency, wines, fruits, meat, fish, sweets, gold, num_deals_purchases, num_web_purchases, num_catalog_purchases, num_store_purchases, num_web_visits_month, age, spent, living_With, children, family_Size, is_Parent)
        group_segment = "3"
        
        st.write("Customer Segment is group: "+ group_segment +"(from call model API on bigqurey)")

        if group_segment == "3":
            st.subheader('Progress Bar for do something continue after ml clustering customer')
    
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

            container.write("From ... Analysis suggestion for this customer group 3 is ... ")    


if __name__ == "__main__":
    main()

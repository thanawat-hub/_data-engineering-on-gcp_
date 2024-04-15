import json
import streamlit as st
import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account

st.title("Titanic Survivor Predictor")

# Define input fields for passenger data
passenger_id_placeholder = "Example: 1, 2, 3, etc..."
passenger_id = st.text_input("PassengerId", value="", help="Enter the passenger's ID.", placeholder=passenger_id_placeholder)

pclass_placeholder = "Example: 1, 2, or 3"
pclass = st.selectbox("Pclass", [1, 2, 3], help="Select the passenger class.", format_func=lambda x: '1st' if x == 1 else '2nd' if x == 2 else '3rd', placeholder=pclass_placeholder)

name_placeholder = "Example: John Doe"
name = st.text_input("Name", value="", help="Enter the passenger's name.", placeholder=name_placeholder)

sex_option_placeholder = "Select Male or Female"
sex_option = st.selectbox("Sex", ["Male", "Female"], help="Select the passenger's gender.", placeholder=sex_option_placeholder)

age_placeholder = "Example: 25"
age = st.number_input("Age", min_value=0, max_value=150, value=30, help="Enter the passenger's age.", placeholder=age_placeholder)

sibsp_placeholder = "Example: 0, 1, 2, etc..."
sibsp = st.number_input("SibSp", min_value=0, value=0, help="Enter the number of siblings/spouses aboard.", placeholder=sibsp_placeholder)

parch_placeholder = "Example: 0, 1, 2, etc..."
parch = st.number_input("Parch", min_value=0, value=0, help="Enter the number of parents/children aboard.", placeholder=parch_placeholder)

ticket_placeholder = "Example: PP 9549, 330877, PC 17599, STON/O2. 3101282"
ticket = st.text_input("Ticket", value="", help="Enter the ticket number.", placeholder=ticket_placeholder)

fare_placeholder = "Example Enter in float: 50.00"
fare = st.number_input("Fare", min_value=0.0, value=50.0, help="Enter the fare.", placeholder=fare_placeholder)

cabin_placeholder = "Example: C23,D10 D12"
cabin = st.text_input("Cabin", value="", help="Enter the cabin number.", placeholder=cabin_placeholder)

embarked_placeholder_help = """
"C" stands for Cherbourg.
"Q" stands for Queenstown (now known as Cobh).
"S" stands for Southampton."""
embarked = st.selectbox("Embarked", ["C", "Q", "S"], help=embarked_placeholder_help)#, placeholder=embarked_placeholder_help)

# Create empty placeholders for feedback messages
feedback_passenger_id = st.empty()
feedback_pclass = st.empty()
feedback_name = st.empty()
feedback_sex_option = st.empty()
feedback_age = st.empty()
feedback_sibsp = st.empty()
feedback_parch = st.empty()
feedback_ticket = st.empty()
feedback_fare = st.empty()
feedback_cabin = st.empty()
feedback_embarked = st.empty()

if st.button("Predict"):
    # Feedback messages for each input field
    feedback_passenger_id.text("Analyzing PassengerId...")
    feedback_pclass.text("Analyzing Pclass...")
    feedback_name.text("Analyzing Name...")
    feedback_sex_option.text("Analyzing Sex...")
    feedback_age.text("Analyzing Age...")
    feedback_sibsp.text("Analyzing SibSp...")
    feedback_parch.text("Analyzing Parch...")
    feedback_ticket.text("Analyzing Ticket...")
    feedback_fare.text("Analyzing Fare...")
    feedback_cabin.text("Analyzing Cabin...")
    feedback_embarked.text("Analyzing Embarked...")

    # Code for prediction goes here
    # Replace this section with your BigQuery ML prediction code

    # Simulating prediction process (replace this with actual prediction code)
    import time
    time.sleep(2)

    # Update feedback messages
    feedback_passenger_id.text("PassengerId analysis completed.")
    feedback_pclass.text("Pclass analysis completed.")
    feedback_name.text("Name analysis completed.")
    feedback_sex_option.text("Sex analysis completed.")
    feedback_age.text("Age analysis completed.")
    feedback_sibsp.text("SibSp analysis completed.")
    feedback_parch.text("Parch analysis completed.")
    feedback_ticket.text("Ticket analysis completed.")
    feedback_fare.text("Fare analysis completed.")
    feedback_cabin.text("Cabin analysis completed.")
    feedback_embarked.text("Embarked analysis completed.")

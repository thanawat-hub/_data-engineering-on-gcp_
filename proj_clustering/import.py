## open terminal ctrl+j

import configparser
import pandas as pd
import pymysql
# from sqlalchemy import create_engine
# Create engine (replace with your credentials)
# engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}') # for uri

CONFIG_FILE = "db.conf"

parser = configparser.ConfigParser()
parser.read(CONFIG_FILE)

database = parser.get("mysql_config", "database")
user = parser.get("mysql_config", "username")
password = parser.get("mysql_config", "password")
host = parser.get("mysql_config", "host")
port = parser.get("mysql_config", "port")

uri = f"mysql+pymysql://{user}:{password}@{host}/{database}"
# เข้าไปตรง uri นี้
path_files_dataset_csv = "clean_clustering.csv"
name2sql = path_files_dataset_csv.split(".")[0]
# print(name2sql)
####name2sql####
df = pd.read_csv(path_files_dataset_csv)
df.to_sql(name2sql, con=uri, if_exists="replace", index=False) 
# เวลา query ต้องใช้ชื่อนี้ Netflix_Userbase_utf_already_preprocessed_dates 
print(f"Imported {name2sql} data successfully")


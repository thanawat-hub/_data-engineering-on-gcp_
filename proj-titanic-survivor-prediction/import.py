## open terminal ctrl+j

import configparser

import pandas as pd


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
df = pd.read_csv("titanic-original.csv")
#### 
df.to_sql("titanic", con=uri, if_exists="replace", index=False)
print(f"Imported titanic data successfully")


# -------------------------------------------------------------------
# คือถ้าใช้ df to sql ลงใน database sql เลย ไม่ต้องกำหนด sql =""" """ คือมันทำให้เลย

# import configparser
# import pandas as pd
# import sqlalchemy
# from sqlalchemy import text

# CONFIG_FILE = "db.conf"

# parser = configparser.ConfigParser()
# parser.read(CONFIG_FILE)

# database = parser.get("mysql_config", "database")
# user = parser.get("mysql_config", "username")
# password = parser.get("mysql_config", "password")
# host = parser.get("mysql_config", "host")
# port = parser.get("mysql_config", "port")

# ####----------####
# # ชื่อ database ต้องไปเอาใน sql gcs 
# #defualt port ของ gps mysql คือ 3306
# # ข้อความระวัง คือรหัส ว่ามันติดชื่อเฉพาะหรือป้าว ข้อสังเกตคือ error ประมาณนี้ 
# # sqlalchemy.exc.OperationalError: (pymysql.err.OperationalError) (2003, "Can't connect to MySQL server on '@34.135.161.94' ([Errno -2] Name or service not known)")
# # ซึ่งสามารถไปเปลี่ยน password ใน gps ได้ ดูใน mem
# ####----------####

# uri = f"mysql+pymysql://{user}:{password}@{host}/{database}"
# engine = sqlalchemy.create_engine(uri)

# # Test SQL command
# # df = pd.read_sql("show tables", engine)
# # print(df.head(3))

# # แนะนำให้ใช้ ตัวเล็กแบบ snake case
# sql ="""
#     CREATE TABLE IF NOT EXISTS titanic (
#         passenger_id  INT,
#         survived      INT,
#         pclass        INT,
#         name          VARCHAR(300),
#         sex	        VARCHAR(10)  ,
#         age	        INT
#     )
#     """
#         # sibSp         INT
#         # parch	      INT
#         # ticket        VARCHAR(100)
#         # fare	      Float
#         # cabin	        
#         # embarked      

# with engine.connect() as conn:
#     conn.execute(text(f"DROP TABLE IF EXISTS titanic"))
#     conn.execute(text(sql))
#     print(f"Created table successfully")

#     # Import to GCS mysql
#     df = pd.read_csv(f"titanic-original.csv")
#     df.to_sql("titanic", con=uri, if_exists="replace", index=False)
#     print(f"Imported titanic data successfully")

#     # # Export
#     # # df = pd.read_sql(f"select * from {k}", engine)
#     # # df.to_csv(f"breakfast_{k}_export.csv", index=False)

# -------------------------------------------------------------------

# import configparser

# import pandas as pd
# import sqlalchemy
# from sqlalchemy import text


# CONFIG_FILE = "db.conf"

# parser = configparser.ConfigParser()
# parser.read(CONFIG_FILE)

# database = parser.get("mysql_config", "database")
# user = parser.get("mysql_config", "username")
# password = parser.get("mysql_config", "password")
# host = parser.get("mysql_config", "host")
# port = parser.get("mysql_config", "port")

# uri = f"mysql+pymysql://{user}:{password}@{host}/{database}"
# engine = sqlalchemy.create_engine(uri)

# sql = """
#     CREATE TABLE IF NOT EXISTS titanic (
#         passenger_id INT,
#         survived     INT,
#         pclass       INT,
#         name         VARCHAR(300)
#     )
# """
# with engine.connect() as conn:
#     conn.execute(text("DROP TABLE IF EXISTS titanic"))
#     conn.execute(text(sql))
#     print("Created table successfully")

#     df = pd.read_csv("titanic-original.csv")
#     df.to_sql("titanic", con=uri, if_exists="replace", index=False)
#     print(f"Imported titanic data successfully")

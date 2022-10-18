from sqlalchemy import create_engine
import pandas as pd
from dotenv import dotenv_values
import os


#path containing credentials
#config = dotenv_values(".env")

#connect to mysql database using credentials

load_dotenv()
MYSQL_HOSTNAME = os.getenv("MYSQL_HOSTNAME")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

connection_string = f'mysql+pymysql://{["MYSQL_USER"]}:{["MYSQL_PASSWORD"]}@{["MYSQL_HOSTNAME"]}/{["MYSQL_DATABASE"]}'
connection_string

db = create_engine(connection_string)
print (db.table_names())

dummy_patients = """
insert into patients values (1, '0001', 'amy', 'zhen', '10003', 
'01/02/2003', 'female'),
insert into patients values (2, '0002', 'andy', 'lee', '10013', 
'01/05/2003', 'male'),
insert into patients values (3, '0003', 'jason', 'ng', '10023', 
'01/12/2003', 'male'),
insert into patients values (4, '0004', 'maria', 'des', '10005', 
'01/08/2003', 'female'),
insert into patients values (5, '0005', 'philip', 'vuey', '10004', 
'01/23/2003', 'male');
"""

db.execute(dummy_patients)
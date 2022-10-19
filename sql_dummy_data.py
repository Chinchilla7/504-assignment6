from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
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

fake_patients = """
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

db.execute(fake_patients)

fake_medications = """
insert into medications values (1, '0001', '1111', '62795-1122-2'),  
insert into medications values (2, '0002', '1112', '54532-0033-5'),
insert into medications values (3, '0003', '1113', '68703-317-30'),
insert into medications values (4, '0004', '1114', '55714-2389-1'),
insert into medications values (5, '0005', '1115', '54532-0032-5')
"""
db.execute(fake_medications)

fake_conditions = """
insert into conditions values (1, '0001', '111111', 'R10.9')
insert into conditions values (2, '0002', '111112', 'R10.1'),
insert into conditions values (3, '0003', '111113', 'R10.8'),
insert into conditions values (4, '0004', '111114', 'R35.0'),
insert into conditions values (5, '0005', '111115', 'R35.1')
"""
db.execute(fake_conditions)

fake_treatments_procedures = """
insert into conditions values (1, '0001', '000001', '99395')
insert into conditions values (2, '0002', '000002', '99395'),
insert into conditions values (3, '0003', '000003', '99395'),
insert into conditions values (4, '0004', '000004', '99395'),
insert into conditions values (5, '0005', '000005', '99395')
"""
db.execute(fake_treatments_procedures)

fake_social_determinants = """
insert into conditions values (1, '0001', '01', '82589-3')
insert into conditions values (2, '0002', '02', '45738-2'),
insert into conditions values (3, '0003', '03', '88124-3'),
insert into conditions values (4, '0004', '04', '87535-1'),
insert into conditions values (5, '0005', '05', '63024-4')
"""
db.execute(fake_social_determinants)


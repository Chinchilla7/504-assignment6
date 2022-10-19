from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os


#path containing credentials
#config = dotenv_values(".env")

#### drop the old tables that do not start with production_
#def droppingFunction_limited(dbList, db_source):
  #  for table in dbList:
   #     if table.startswith('production_') == False:
   #         db_source.execute(f'drop table {table}')
    #        print(f'dropped table {table}')
     #   else:
      #      print(f'kept table {table}')

#def droppingFunction_all(dbList, db_source):
   # for table in dbList:
   #     db_source.execute(f'drop table {table}')
   #     print(f'dropped table {table}')
   # else:
   #     print(f'kept table {table}')

#connect to mysql database using credentials

load_dotenv()

MYSQL_HOSTNAME = os.getenv("MYSQL_HOSTNAME")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

connection_string = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}:3306/{MYSQL_DATABASE}'
connection_string

db = create_engine(connection_string)
print (db.table_names())

## reoder tables: production_patient_conditions, production_patient_medications, production_medications, production_patients, production_conditions
#tableNames = ['production_patient_conditions', 'production_patient_medications', 'production_medications', 'production_patients', 'production_conditions']

## ### delete everything 
#droppingFunction_all(tableNames, db)


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
insert into medications values (1, '0001', '62795-1122-2'),  
insert into medications values (2, '0002', '54532-0033-5'),
insert into medications values (3, '0003', '68703-317-30'),
insert into medications values (4, '0004', '55714-2389-1'),
insert into medications values (5, '0005', '54532-0032-5')
"""
db.execute(fake_medications)

fake_conditions = """
insert into conditions values (1, 'R10.9')
insert into conditions values (2, 'R10.1'),
insert into conditions values (3, 'R10.8'),
insert into conditions values (4, 'R35.0'),
insert into conditions values (5, 'R35.1')
"""
db.execute(fake_conditions)

fake_treatments_procedures = """
insert into conditions values (1, '0001', '99395')
insert into conditions values (2, '0002', '99395'),
insert into conditions values (3, '0003', '99395'),
insert into conditions values (4, '0004', '99395'),
insert into conditions values (5, '0005', '99395')
"""
db.execute(fake_treatments_procedures)

fake_social_determinants = """
insert into conditions values (1, '0001', '82589-3')
insert into conditions values (2, '0002', '45738-2'),
insert into conditions values (3, '0003', '88124-3'),
insert into conditions values (4, '0004', '87535-1'),
insert into conditions values (5, '0005', '63024-4')
"""
db.execute(fake_social_determinants)


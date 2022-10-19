#create tables: patients, medications, treatments_procedures, conditions, and social determinants
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os



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
#creating new tables within patient_portal database called patients, medications, treatments_procedures, 
#conditions, and social determinants.  
#TABLENAME = MYSQL_USER + 'fakeTableAssignment1'

create_Patients_table = """
create table IF NOT EXISTS patients (
  id int,
  mrn varchar(255),
  first_name varchar(255),
  last_name varchar(255),
  zip_code varchar(255),
  dob varchar(255),
  gender varchar(255),
  PRIMARY KEY (id),
);
"""

db.execute(create_Patients_table)

create_medications_table= """
create table IF NOT EXISTS medications (
  id int,
  mrn varchar(255),
  medication_id varchar(255),
  ndc_codes varchar(255),
  PRIMARY KEY (id),
  );
  """
db.execute(create_medications_table)

create_conditions_table= """
create table IF NOT EXISTS conditions (
  id int,
  mrn varchar(255),
  condition_id varchar(255),
  icd_10_codes varchar(255),
  PRIMARY KEY (id),
  );
  """
db.execute(create_conditions_table)

create_treatments_procedures_table= """
create table IF NOT EXISTS treatments_procedures (
  id int,
  mrn varchar(255),
  treatments_procedures_id varchar(255),
  cpt_codes varchar(255),
  PRIMARY KEY (id),
  );
  """
db.execute(create_treatments_procedures_table)

create_social_determinants_table= """
create table IF NOT EXISTS social_determinants (
  id int,
  mrn varchar(255),
  social_determinants_id varchar(255),
  loinc_codes varchar(255),
  PRIMARY KEY (id),
  );
  """
db.execute(create_social_determinants_table)










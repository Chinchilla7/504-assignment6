#create tables: patients, medications, treatments_procedures, conditions, and social determinants
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import dbm


#### drop the old tables that do not start with 
def droppingFunction_limited(dbList, db_source):
    for table in dbList:
        if table.startswith('') == False:
            db_source.execute(f'drop table {table}')
            print(f'dropped table {table}')
        else:
            print(f'kept table {table}') 

def droppingFunction_all(dbList, db_source):
    for table in dbList:
        db_source.execute(f'drop table {table}')
        print(f'dropped table {table}')
    else:
        print(f'kept table {table}')

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

create_Patients_table = """
create table IF NOT EXISTS patients (
  id int auto_increment,
  mrn varchar(255) default null unique,
  first_name varchar(255) default null,
  last_name varchar(255) default null,
  zip_code varchar(255) default null,
  dob varchar(255) default null,
  gender varchar(255) default null,
  PRIMARY KEY (id),
);
"""

db.execute(create_Patients_table)

create_medications_table= """
create table IF NOT EXISTS medications (
  id int auto_increment,
  medication_name varchar(255) default null,
  ndc_codes varchar(255) default null unique,
  PRIMARY KEY (id),
  );
  """
db.execute(create_medications_table)

create_conditions_table= """
create table IF NOT EXISTS conditions (
  id int auto_increment,
  icd_10_codes varchar(255) default null unique,
  icd_description varchar(255) default null,
  PRIMARY KEY (id),
  );
  """
db.execute(create_conditions_table)

create_treatments_procedures_table= """
create table IF NOT EXISTS treatments_procedures (
  id int auto_increment,
  treatments_procedures_desciption varchar(255),
  cpt_codes varchar(255) default null unique,
  PRIMARY KEY (id),
  );
  """
db.execute(create_treatments_procedures_table)

create_social_determinants_table= """
create table IF NOT EXISTS social_determinants (
  id int auto_increment,
  social_determinants_description varchar(255),
  loinc_codes varchar(255) default null unique,
  PRIMARY KEY (id),
  );
  """
db.execute(create_social_determinants_table)

create_Patients_medications = """
create table IF NOT EXISTS patients_medication (
  id int auto_increment,
  mrn varchar(255) default null,
  ndc_codes varchar(255) default null,
  PRIMARY KEY (id),
  FOREIGN KEY (mrn) REFERNCES patients(mrn) ON DELETE CASCADE,
  FOREIGN KEY (ndc_codes) REFERNCES medications(ndc_codes) ON DELETE CASCADE,
);
"""
db.execute(create_Patients_medications)

create_Patients_conditions = """
create table IF NOT EXISTS patients_conditions (
  id int auto_increment,
  mrn varchar(255) default null,
  icd_10_codes varchar(255) default null,
  PRIMARY KEY (id),
  FOREIGN KEY (mrn) REFERNCES patients(mrn) ON DELETE CASCADE,
  FOREIGN KEY (icd_10_codes) REFERNCES medications(icd_10_codes) ON DELETE CASCADE,
);
"""
db.execute(create_Patients_conditions)







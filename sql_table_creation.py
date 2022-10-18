#create tables: patients, medications, treatments_procedures, conditions, and social determinants
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

#creating a new table within patient_portal database called patients containing data from iris csv 
TABLENAME = MYSQL_USER + 'fakeTableAssignment1'

create_Patients_table= """
create table IF NOT EXISTS patients (
  id int,
  mrn varchar(255),
  f_name varchar(255),
  last_name varchar(255),
  zip_code varchar(255),
  dob varchar(255),
  gender varchar(255),
  drug_id int ,
  PRIMARY KEY (id),
  FOREIGN KEY (drug_id) REFERENCES medications(id)
);
"""

db.execute(create_Patients_table)

create_medications_table= """
create table IF NOT EXISTS medications (
  id int,
  mrn varchar(255),
  medication_id varchar(255),
  PRIMARY KEY (id),
  );
  """
db.execute(create_medications_table)

create_conditions_table= """
create table IF NOT EXISTS conditions (
  id int,
  mrn varchar(255),
  condition_id varchar(255),
  PRIMARY KEY (id),
  );
  """
db.execute(create_conditions_table)




r_df = pd.read_csv('')
r_df.to_sql('patients', con=db, if_exists='replace')

sql_query = 'SELECT * from  where =""'

results =pd.read_sql(sql_query, con=db)
results








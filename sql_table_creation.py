#create tables: patients, medications, treatments_procedures, conditions, and social determinants
from sqlalchemy import create_engine
import pandas as pd
from dotenv import dotenv_values

#path containing credentials
config = dotenv_values(".env")

#connect to mysql database using credentials

connection_string = f'mysql+pymysql://{config["MYSQL_USER"]}:{config["MYSQL_PASSWORD"]}@{config["MYSQL_HOSTNAME"]}/{config["MYSQL_DATABASE"]}'
connection_string

db = create_engine(connection_string)
print (db.table_names())

#creating a new table within patient_portal database called patients containing data from iris csv 
r_df = pd.read_csv('')
r_df.to_sql('patients', con=db, if_exists='replace')

sql_query = 'SELECT * from  where =""'

results =pd.read_sql(sql_query, con=db)
results








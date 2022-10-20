from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
from faker import Faker 
import uuid
import random
import dbm



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


#### fake stuff 
fake = Faker()

fake_patients = [
    {
        #keep just the first 8 characters of the uuid
        'mrn': str(uuid.uuid4())[:8], 
        'first_name':fake.first_name(), 
        'last_name':fake.last_name(),
        'zip_code':fake.zipcode(),
        'dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
        'gender': fake.random_element(elements=('M', 'F'))
    } for x in range(10)]

df_fake_patients = pd.DataFrame(fake_patients)
# drop duplicate mrn
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])


#### real icd10 codes
icd10codes = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10codes.columns)
icd10codesShort = icd10codes[['CodeWithSeparator', 'ShortDescription']]
icd10codesShort_1k = icd10codesShort.sample(n=1000, random_state=1)
# drop duplicates
icd10codesShort_1k = icd10codesShort_1k.drop_duplicates(subset=['CodeWithSeparator'], keep='first')



#### real ndc codes
ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1)
# drop duplicates from ndc_codes_1k
ndc_codes_1k = ndc_codes_1k.drop_duplicates(subset=['PRODUCTNDC'], keep='first')


# real cpt codes
cpt_codes = pd.read_csv('')
cpt_codes_1k = cpt_codes.sample(n=1000, random_state=1)
# drop duplicates from ndc_codes_1k
cpt_codes_1k = cpt_codes_1k.drop_duplicates(subset=[''], keep='first')


# real loinc codes
loinc_codes = pd.read_csv('')
loinc_codes_1k = cpt_codes.sample(n=1000, random_state=1)
# drop duplicates from ndc_codes_1k
loinc_codes_1k = cpt_codes_1k.drop_duplicates(subset=[''], keep='first')


#Insert fake patients into table patients
insertQuery = "INSERT INTO production_patients (mrn, first_name, last_name, zip_code, dob, gender, contact_mobile, contact_home) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

for index, row in df_fake_patients.iterrows():
    db.execute(insertQuery, (row['mrn'], row['first_name'], row['last_name'], row['zip_code'], row['dob'], row['gender'], row['contact_mobile'], row['contact_home']))
    print("inserted row: ", index)

# # query dbs to see if data is there
df_gcp = pd.read_sql_query("SELECT * FROM production_patients", db)

#Insert conditions
insertQuery = "INSERT INTO conditions (icd_10_codes, icd_description) VALUES (%s, %s)"

startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow += 1
    db.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_gcp: ", index)
    ## stop once we have 50 rows
    if startingRow == 50:
        break

# query dbs to see if data is there
df_gcp = pd.read_sql_query("SELECT * FROM production_conditions", db)

#Insert medications into medications table
insertQuery = "INSERT INTO production_medications (ndc_codes, medication_name) VALUES (%s, %s)"

medRowCount = 0
for index, row in ndc_codes_1k.iterrows():
    medRowCount += 1
    db.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    print("inserted row: ", index)
    ## stop once we have 50 rows
    if medRowCount == 50:
        break


# query dbs to see if data is there
df_gcp = pd.read_sql_query("SELECT * FROM production_medications", db)

#Insert treatments/procedures into treatments_procedures table
insertQuery = "INSERT INTO treatments_procedures (treatments_procedures_desciption, cpt_codes) VALUES (%s, %s)"

medRowCount = 0
for index, row in cpt_codes_1k.iterrows():
    medRowCount += 1
    db.execute(insertQuery, (row[''], row['']))
    print("inserted row: ", index)
    ## stop once we have 50 rows
    if medRowCount == 50:
        break


# query dbs to see if data is there
df_gcp = pd.read_sql_query("SELECT * FROM social_determinants", db)

#Insert social determinants into social determinants table
#social_determinants_description
insertQuery = "INSERT INTO social_determinants (social_determinants_description, loinc_codes) VALUES (%s, %s)"

medRowCount = 0
for index, row in loinc_codes_1k.iterrows():
    medRowCount += 1
    db.execute(insertQuery, (row[''], row['']))
    print("inserted row: ", index)
    ## stop once we have 50 rows
    if medRowCount == 50:
        break


# query dbs to see if data is there
df_gcp = pd.read_sql_query("SELECT * FROM social_determinants", db)



#Creating fake patient medication records
# query medications and patients to get the ids

df_medications = pd.read_sql_query("SELECT ndc_codes FROM medications", db) 
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db)

# create a dataframe that is stacked and give each patient a random number of medications between 1 and 5
df_patient_medications = pd.DataFrame(columns=['mrn', 'ndc_codes'])
# for each patient in df_patient_medications, take a random number of medications between 1 and 10 from df_medications and palce it in df_patient_medications
for index, row in df_patients.iterrows():
    # get a random number of medications between 1 and 5
    numMedications = random.randint(1, 5)
    # get a random sample of medications from df_medications
    df_medications_sample = df_medications.sample(n=numMedications)
    # add the mrn to the df_medications_sample
    df_medications_sample['mrn'] = row['mrn']
    # append the df_medications_sample to df_patient_medications
    df_patient_medications = df_patient_medications.append(df_medications_sample)

print(df_patient_medications)

# now lets add a random medication to each patient
insertQuery = "INSERT INTO production_patient_medications (mrn, ndc_codes) VALUES (%s, %s)"

for index, row in df_patient_medications.iterrows():
    db(insertQuery, (row['mrn'], row['ndc_codes']))
    print("inserted row: ", index)


#Creaating fake patient conditions
# query conditions and patients to get the ids
df_conditions = pd.read_sql_query("SELECT icd_10_codes FROM conditions", db)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db)

# create a dataframe that is stacked and give each patient a random number of conditions between 1 and 5
df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd_10_codes'])
# for each patient in df_patient_conditions, take a random number of conditions between 1 and 10 from df_conditions and palce it in df_patient_conditions
for index, row in df_patients.iterrows():
    # get a random number of conditions between 1 and 5
    numConditions = random.randint(1, 5)
    # get a random sample of conditions from df_conditions
    df_conditions_sample = df_conditions.sample(n=numConditions)
    # add the mrn to the df_conditions_sample
    df_conditions_sample['mrn'] = row['mrn']
    # append the df_conditions_sample to df_patient_conditions
    df_patient_conditions = df_patient_conditions.append(df_conditions_sample)

print(df_patient_conditions)

# now lets add a random condition to each patient
insertQuery = "INSERT INTO production_patient_conditions (mrn, icd_10_codes) VALUES (%s, %s)"

for index, row in df_patient_conditions.iterrows():
    db(insertQuery, (row['mrn'], row['icd10_code']))
    print("inserted row: ", index)






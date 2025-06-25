import pandas as pd
import json
from sqlalchemy import create_engine

# read the excel file
df = pd.read_excel("/home/anandawln/my-airflow-project/Student_Performance.xlsx")
print("DataFrame Preview:")
print(df.head())
print(f"Number of rows to be inserted: {len(df)}")

# ambil credential 
with open('/home/anandawln/my-airflow-project/credential.json') as f:
    credential = json.load(f)

username = credential['username']
password = credential['password']
host = credential['host']
port = credential['port']
database = credential['database']

# create engine connection to postgres
engine = create_engine(f"postgresql+psycopg2://{credential['username']}:{credential['password']}@{credential['host']}:{credential['port']}/{credential['database']}")

# ingest data to postgres
df.to_sql('student_performance', engine, if_exists='replace', index=False)



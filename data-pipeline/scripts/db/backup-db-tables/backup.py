import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
import configparser


config = configparser.ConfigParser()
config.read(r'../../../config.ini')

# DATABASE

db      = config['MySQL']['db']
db_pass = config['MySQL']['db_pass']
db_host = config['MySQL']['db_host']
db_user = config['MySQL']['db_user']

# ENGINE
connct_str = f"""mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}/{db}"""
engine = create_engine(connct_str)

# TEST CONNECTION
try:
    with engine.connect() as connection:
        if connection:
            print('Connected to MySQL Database succesfully')
except Exception as e:
    print(f'Error connecting to MySQL Database {e}')
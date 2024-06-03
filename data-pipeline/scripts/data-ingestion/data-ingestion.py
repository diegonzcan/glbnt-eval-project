import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
import logging
import configparser

config = configparser.ConfigParser()
config.read(r'../../config.ini')

logging.basicConfig(filename='std.log',
                    filemode='a',
                    format='%(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)

# CONSTANTS 

# DATABASE

db      = config['MySQL']['db']
db_pass = config['MySQL']['db_pass']
db_host = config['MySQL']['db_host']
db_user = config['MySQL']['db_user']

# DATAFILES
file_path = config['PATHS']['data_sink']

jobs = 'jobs.csv'
employees = 'hired_employees.csv'
departments = 'departments.csv'

# ENGINE
connct_str = f"""mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}/{db}"""
engine = create_engine(connct_str)
metadata = MetaData()

# CREATE TABLE OBJECTS
jobs_table = Table('jobs', metadata, autoload_with=engine)
departments_table = Table('departments', metadata, autoload_with=engine)
hired_employees_table = Table('hired_employees', metadata, autoload_with=engine)

# START SESSION
session = sessionmaker(bind=engine)
session = session()

# TEST CONNECTION
try:
    with engine.connect() as connection:
        if connection:
            logging.info('Connected to MySQL Database succesfully')
except Exception as e:
    logging.info(f'Error connecting to MySQL Database {e}')

def insert_csv(file,destination_table):
    "Sets the path of the data files and the corresponding mysql destination table"
    full_path = Path(file_path) / file
    try:
        with engine.begin() as connection:
            data = pd.read_csv(full_path)
            data.to_sql(destination_table, con=connection,if_exists='append', index=False)
            row_size = data.shape[0]
            logging.info(f'Succesfully loaded {row_size} rows from {file} to {destination_table}')
    except Exception as e:
        logging.info(f'Error reading or inserting data {e}')
    logging.info("files loaded")
    

if __name__ == '__main__':
    try:
        insert_csv(jobs, 'jobs')
        insert_csv(employees, 'hired_employees')
        insert_csv(departments, 'departments')
    except Exception as e:
        logging.exception(f"An error occurred during data insertion: {e}")
        logging.info("Error occurred during data insertion. Check log for details.")

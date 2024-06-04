import pandas as pd
import sys
import os
import logging
import configparser
from pathlib import Path

# LOG RESET
if os.path.exists('std.log'):
    os.remove('std.log')
else: pass

logging.basicConfig(filename='std.log',
                    filemode='a',
                    format='%(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)

config = configparser.ConfigParser()
config.read(r'../../config.ini')

# DATA FILES
jobs = 'jobs.csv'
hired_employees = 'hired_employees.csv'
departments = 'departments.csv'

# DATA PATH
data_path = config['PATHS']['data']
data_sink = config['PATHS']['data_sink']

def get_data_range(data):
    """Gets data ranges for string and numerical columns and logs them to std.log file
       We will use this to determine SQL table DDL 
     """

    for c in data.select_dtypes(include=['object']).columns:
        max_length = data[c].apply(lambda x: len(str(x))).max()
        min_length = data[c].apply(lambda x: len(str(x))).min()
        logging.info(f'Column: {c}\n Character range:{min_length} - {max_length} ')

    for c in data.select_dtypes(include=['int64', 'float64']):
        max = data[c].max()
        min = data[c].min()
        logging.info(f'Column: {c}\n Numerical range:{min} - {max} ')

data_files = {
jobs: ['id','job'],
departments: ['id', 'department'],
hired_employees: ['id','name','datetime', 'department_id','job_id']
}

for data_file, schema in data_files.items():
    # We generate the csv files with the destination table schema to a staging area to be inserted to mysql 
    logging.info(f'{data_file}')
    data = pd.read_csv(Path(data_path) / data_file, header=None, names=schema)  # we set the columns to the schema provided
    data.to_csv(Path(data_sink) / data_file, index=False)
    get_data_range(data=data)
from sqlalchemy import create_engine, text
import configparser
from fastavro import parse_schema, writer

config = configparser.ConfigParser()
config.read(r'../../../config.ini')

# DATABASE

db      = config['MySQL']['db']
db_pass = config['MySQL']['db_pass']
db_host = config['MySQL']['db_host']
db_user = config['MySQL']['db_user']

# AVRO FILES PATH

PATH = config['PATHS']['data_bckp']

# ENGINE
connct_str = f"""mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}/{db}"""
engine = create_engine(connct_str, echo=True)

# TEST CONNECTION
try:
    with engine.connect() as connection:
        if connection:
            print('Connected to MySQL Database succesfully')
except Exception as e:
    print(f'Error connecting to MySQL Database {e}')

records = []
job_schema = {
    'doc': 'jobs',
    'name': 'jobs',
    'namespace': 'jobsdata',
    'type': 'record',
    'fields': [
        {'name': 'id', 'type': 'int'},
       {'name': 'job', 'type': 'string'} 
    ]
}

department_schema = {
    "type": "record",
    "name": "departments",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "department", "type": "string"}
    ]
}

hired_employees_schema = {
    "type": "record",
    "name": "hired_employees",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type":"string"},
        {"name": "datetime", "type": "string"},
        {"name": "department_id", "type": "int"},
        {"name": "job_id", "type":"int"}
    ]
}


def backup_table(table, schema):
    parsed_schema = parse_schema(schema)

    records = []
    with engine.connect() as con:
        result = con.execute(text(f'SELECT * FROM {table}'))
        for  row in result.mappings():
                records.append(row)
        with open(PATH + '/' + f'{table}.avro', 'wb') as out:
            writer(out, parsed_schema, records)

if __name__ == '__main__':
    backup_table('jobs', job_schema)
    backup_table('departments', department_schema)
    backup_table('hired_employees', hired_employees_schema)
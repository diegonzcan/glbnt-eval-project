from airflow import DAG
from airflow.operators.python_operator import PythonOperator, DummyOperator
from datetime import datetime

default_args = {
    'owner': 'diego',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 7),
    'retries': 1
}

dag = DAG(
    'backup_tables',
    default_args=default_args,
    description='A DAG to backup database tables daily',
    schedule_interval='@daily',
)

def run_backup_tables():
    import sys
    sys.path.append('../scripts/db/backup-db-tables/')
    import backup_tables  

    # Call backup_tables.py script
    backup_tables.main()

backup_task = PythonOperator(
    task_id='run_backup_tables',
    python_callable=run_backup_tables,
    dag=dag,
)

start = DummyOperator(task_id = 'start', dag = dag)

end = DummyOperator(task_id = 'end', dag = dag)


start >> backup_task >> end

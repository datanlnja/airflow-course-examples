from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

# Создадим объект класса DAG
dag =  DAG('3.2.3', schedule_interval=timedelta(days=1), start_date=days_ago(1))

def print_context():
    return 'Hello World'

run_this = PythonOperator(
    task_id='print_the_context',
    python_callable=print_context,
    dag=dag,
)
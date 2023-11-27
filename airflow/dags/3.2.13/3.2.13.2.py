from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import pandas as pd

def f_callable(url, f_name):
  data = pd.read_csv(url)
  data.to_csv(f_name)

with DAG('3.2.13.2', schedule_interval=timedelta(days=1), start_date=days_ago(1)) as dag:
  # Создадим оператор для исполнения python функции
  t1 = PythonOperator(task_id='download_file',
                      python_callable=f_callable,
                      op_kwargs={'url': 'https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data/data.csv',
                                 'f_name': '/root/airflow/data.csv'})
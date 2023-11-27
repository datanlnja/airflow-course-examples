from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
 
# Создадим объект класса DAG
dag =  DAG('3.2.2', schedule_interval=timedelta(days=1), start_date=days_ago(1))
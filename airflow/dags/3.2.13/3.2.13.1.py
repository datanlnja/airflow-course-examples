from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

# Создадим объект класса DAG
dag = DAG('3.2.13.1', schedule_interval=timedelta(days=1))

# Создадим несколько шагов
t1 = DummyOperator(task_id='echo_1', dag=dag)
t2 = DummyOperator(task_id='echo_2', dag=dag)
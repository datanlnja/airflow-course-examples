from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

def f_callable():
  print("Hello World!")

dag = DAG('3.2.8',schedule_interval=timedelta(days=1), start_date=days_ago(1))
# Создадим оператор для исполнения python функции
t1 = PythonOperator(task_id='print', python_callable=f_callable,dag=dag)

# Документация
t1.doc_md = "Task Documentations :)"
dag.doc_md = "Dag Documentations :)"
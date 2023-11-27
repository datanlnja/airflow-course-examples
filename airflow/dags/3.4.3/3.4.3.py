from airflow import DAG 
from datetime import datetime 
from airflow.operators.python_operator import PythonOperator 

default_args = {
   "owner": "airflow", 
   "start_date": datetime(2020, 9, 19)
}
 
dag = DAG(
   dag_id="3.4.3", 
   default_args=default_args, 
   schedule_interval="@daily"
) 

# Функция использующая контекст 
def _print_exec_date(date, **context): 
    print("Дата запуска задачи", date) 
    
task = PythonOperator(
  task_id='task1',
  python_callable=_print_exec_date ,
  op_kwargs={
      'date': '{{ ds }}',
  },
  dag=dag)
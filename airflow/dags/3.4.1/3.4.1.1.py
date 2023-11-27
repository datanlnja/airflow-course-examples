from airflow import DAG 
from datetime import datetime 
from airflow.operators.python_operator import PythonOperator 

default_args = {
   "owner": "airflow", 
   "start_date": datetime(2018, 10, 1)
}
 
dag = DAG(
   dag_id="3.4.1.1", 
   default_args=default_args, 
   schedule_interval="@daily"
) 

# Функция использующая контекст 
# **args/**context это словарь в который будет помещен
# Контекст задачи
def _print_exec_date(**context): 
    print("Контекст", context) 
    
print_exec_date = PythonOperator( 
    task_id="print_exec_date", 
    python_callable=_print_exec_date, 
    dag=dag, )
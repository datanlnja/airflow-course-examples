from airflow import DAG 
from datetime import datetime 
from airflow.operators.bash_operator import BashOperator 

default_args = {"owner": "airflow", "start_date": datetime(2018, 10, 1)} 
dag = DAG(dag_id="3.4.1.2", default_args=default_args, schedule_interval="@daily") 

bash_task = BashOperator( 
    task_id="bash_task", 
    bash_command='echo "Context: \'$message\'"', 
    env={'message': '{{ execution_date }}'}, # Используется jinja выражение
    dag=dag )
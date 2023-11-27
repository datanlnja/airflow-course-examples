from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

default_args = {"pool":'one_task_pool'}

dag = DAG('4.1.4.only_one', default_args=default_args,schedule_interval=timedelta(days=1), start_date=days_ago(1))
t1 = BashOperator(
    task_id='task_1',
    bash_command="echo 1",
    dag=dag,
)
t2 = BashOperator(
    task_id='task_2',
    bash_command="echo 1",
    dag=dag,
)
t3 = BashOperator(
    task_id='task_3',
    bash_command="echo 1",
    dag=dag,
)
t4 = BashOperator(
    task_id='task_4',
    bash_command="echo 1",
    dag=dag,
)
t5 = BashOperator(
    task_id='task_5',
    bash_command="echo 1",
    dag=dag,
)
t6 = BashOperator(
    task_id='task_6',
    bash_command="echo 1",
    dag=dag,
)
t7 = BashOperator(
    task_id='task_7',
    bash_command="echo 1",
    dag=dag,
)

[t1, t2]>>t5
t3>>t6
[t5,t6] >>  t7
t4
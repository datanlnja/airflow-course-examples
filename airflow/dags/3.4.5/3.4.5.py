from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

dag = DAG('3.4.5',schedule_interval=timedelta(days=1), start_date=days_ago(1))

downloading_data = BashOperator(
    task_id='downloading_data',
    bash_command='echo "Hello, I am a value!"',
    do_xcom_push=True, # Результат работы будет отправлен в Xcom, в новых версиях это необязательный параметр и данные будут отправлены в Xcom, по умочланию
    dag=dag
)

fetching_data = BashOperator(
    task_id='fetching_data',
    # Через Jinja также можно отправлять xcom
    bash_command="echo 'XCom fetched: {{ ti.xcom_pull(task_ids=[\'downloading_data\']) }}'",
    dag=dag
)

downloading_data >> fetching_data
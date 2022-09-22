import os
import sys
from datetime import datetime, timedelta
import datetime as dt
import importlib

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
# from cloud_training import LongRunningTasks
# Parameteres
WORFKLOW_DAG_ID = "check_retro_gui_dag"
WORFKFLOW_START_DATE = dt.datetime(2022, 9, 2, 3, 22, 00)
WORKFLOW_SCHEDULE_INTERVAL =  "*/1 * * * *"
WORKFLOW_EMAIL = ["safiullah@retrocausal.ai"]

WORKFLOW_DEFAULT_ARGS = {
    "owner": "taban",
    # "start_date": dt.datetime(2022, 9, 5, 11, 36, 00),
    "email": WORKFLOW_EMAIL,
    "email_on_failure": True,
    "email_on_retry": False,
    # "retries": 0
}
# Initialize DAG
dag = DAG(
    dag_id=WORFKLOW_DAG_ID,
    default_args=WORKFLOW_DEFAULT_ARGS,
    start_date=datetime(2022, 1, 1),
    schedule_interval='@daily',
    catchup=False
)


def check_retro_gui():
    # there are changes
    # goto /opt/airflow/retro
    # git pull
    # cp dags folder from retro to /opt/airflow/dags
    spam_loader = importlib.find_loader('cloud_training')
    found = spam_loader is not None
    print(found)
    print("getcwd:", os.getcwd())
    print(os.listdir())
    print(sys.path)
    sys.path.append('/retroactivity')
    print(sys.path)
    os.chdir("..")
    print("getcwd:", os.getcwd())
    print(os.listdir())
    return ''
#
# check_retro_gui_operator
# bash_task = BashOperator(task_id='bash_task', bash_command="git -c http.extraHeader='Authorization: Basic OnJxanBvY3ZrYWQyd21qbmF4M2ljamd4d2NrcHc0dG55cW52bzMzanRlaXRkYjdmaTJ6NnE=' clone https://andrey0942@dev.azure.com/andrey0942/Retroactivity/_git/Retroactivity")
# get_latest_commit_task = PythonOperator(
#                         task_id='get_latest_commit_task',
#                         python_callable=functools.partial(get_latest_commit, branch="apache-airflow"),
#                         dag=dag
#                     )
# bash_task = BashOperator(task_id='bash_task', bash_command="ls")

check_retro_gui_operator = PythonOperator(task_id='check_retro_gui_operator',
                                          python_callable=check_retro_gui,
                                          dag=dag)
#
# check_retro_gui_operator
send_sqs_message = BashOperator(
        task_id='send_sqs_message',
        bash_command='python /opt/***/retroactivity/send_sqs_message.py',
        execution_timeout=timedelta(minutes=2),
        dag=dag)

cloud_training = BashOperator(
        task_id='cloud_training',
        bash_command='python /opt/***/retroactivity/cloud_training.py',
        execution_timeout=timedelta(minutes=2),
        dag=dag)

check_retro_gui_operator >> send_sqs_message >> cloud_training

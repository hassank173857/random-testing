import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from subprocess import PIPE, CalledProcessError, Popen
import requests
import base64
import json

from git_helper_function import stash_current_branch, delete_local_branch, fetch_all, switch_branch, \
    take_latest_pull, set_git_credentials, perform_stash

username = os.getenv('USER')
user_pat = os.getenv('PAT')
authorization = str(base64.b64encode(bytes(':'+"rqjpocvkad2wmjnax3icjgxwckpw4tnyqnvo33jteitdb7fi2z6q", 'ascii')), 'ascii')
print("auth", authorization)
headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Basic auth '+authorization
}

# Parameteres
WORFKLOW_DAG_ID = "check_retro_gui_dag2"
WORFKFLOW_START_DATE = datetime(2022, 9, 5, 12, 3, 00),
WORKFLOW_SCHEDULE_INTERVAL =  "*/10 * * * *"
# WORKFLOW_SCHEDULE_INTERVAL =  "@daily"
WORKFLOW_EMAIL = ["safiullah@retrocausal.ai"]
BRANCH = "apache-airflow"
WORKFLOW_DEFAULT_ARGS = {
    # "owner": "taban",
    "start_date": datetime(2022, 9, 5),
    # "email": WORKFLOW_EMAIL,
    # "email_on_failure": True,
    # "email_on_retry": False,
    # "retries": 0
}
# Initialize DAG
dag = DAG(
    dag_id=WORFKLOW_DAG_ID,
    start_date=datetime(2022, 9, 6, 8, 6, 00),
    schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,
    catchup=False
)


def get_latest_remote_commit_date() -> bool:
    date_remote = get_latest_commit_remote()

    date_local = get_latest_commit_local()
    print("date_local:", date_local)
    print("date_remote:", date_remote)
    if date_remote > date_local:
        return True
    return False

def get_latest_commit_local():
    # for local
    os.chdir("./retroactivity")
    print("cwd:", os.getcwd())
    process = Popen("git log -1 " + BRANCH + ' --pretty=format:"%ci"', shell=True, stdout=PIPE)
    out, err = process.communicate()
    if not err:
        date_local = out.decode("utf-8")
        date_local = date_local.split('+')[0].replace('"', '').strip()
        date_local = datetime.strptime(date_local, '%Y-%m-%d %H:%M:%S')
        return date_local

    return datetime.now()


def get_latest_commit_remote():
    # for remote
    response = requests.get(
        url=f"https://dev.azure.com/andrey0942/Retroactivity/_apis/git/repositories/Retroactivity/commits?searchCriteria.itemVersion.version={BRANCH}",
        headers=headers)
    data = json.loads(response.content)
    date_remote = data['value'][0]['author']['date']
    date_remote = datetime.strptime(date_remote, '%Y-%m-%dT%H:%M:%SZ')
    date_remote = date_remote + timedelta(hours=5)
    print("latest commit", data['value'][0])
    return date_remote


def perform_pull(ti) -> bool:
    if ti.xcom_pull(task_ids='get_latest_commit_task'):
        try:
            if os.path.isdir("./retroactivity"):
                os.chdir("./retroactivity")
                # stash_current_branch()
                # delete_local_branch()
                # fetch_all()
                # switch_branch()
                set_git_credentials()
                perform_stash()
                take_latest_pull()
                process = Popen(f"git log -1", shell=True, stdout=PIPE)
                out, err = process.communicate()
                print("git log:", out, err)
                return True
        except CalledProcessError as e:
            print("error")
            print(e.output)  # Output: Command 'exit 1' returned non-zero exit status 1.
            return False
    else:
        print("No new Commits! Already upto Date.")
        return False

def update_dag_folder(ti):
    print("cwd:", os.getcwd())
    process = Popen(f"ls", shell=True, stdout=PIPE)
    out, err = process.communicate()
    print("ls:", out, err)
    if ti.xcom_pull(task_ids='perform_pull_task'):
        if os.path.isdir("./retroactivity/airflow/dags"):
            process = Popen(f"cp -a retroactivity/airflow/dags/. dags", shell=True, stdout=PIPE)
            out, err = process.communicate()
            print("copy:", out, err)
    else:
        print("Dags folder has no updates!")


get_latest_commit_task = PythonOperator(
    task_id='get_latest_commit_task',
    python_callable=get_latest_remote_commit_date,
    dag=dag
)

perform_pull_task = PythonOperator(
    task_id='perform_pull_task',
    python_callable=perform_pull,
    dag=dag
)

update_dags_folder = PythonOperator(
    task_id='update_dags_folder',
    python_callable=update_dag_folder,
    dag=dag
)

get_latest_commit_task >> perform_pull_task >> update_dags_folder


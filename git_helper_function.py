import base64
import os
from subprocess import Popen, PIPE
# from dotenv import load_dotenv
# load_dotenv()
# from airflow.dags.check_changes_dag2 import BRANCH, url
username = os.getenv('USER')
user_pat = os.getenv('PAT')
# url = f"https://{username}:{user_pat}@dev.azure.com/andrey0942/Retroactivity/_git/Retroactivity"
# "https://safiullah:rqjpocvkad2wmjnax3icjgxwckpw4tnyqnvo33jteitdb7fi2z6q@dev.azure.com/andrey0942/Retroactivity/_git/Retroactivity"
BRANCH = "apache-airflow"

print(username, user_pat)

def stash_current_branch():
    print(f"current working dir: {os.getcwd()}")
    print("Stashing current branch")
    process = Popen(f"git stash --keep-index", shell=True, stdout=PIPE)
    process.wait()
    if process.returncode != 0:
        print(f"Failed to stash current branch")
        raise Exception
    print("Current branch stashed successfully")


def delete_local_branch():
    process = Popen(f"git branch -d {BRANCH}", shell=True, stdout=PIPE)
    process.wait()
    print(process.stdout.read())
    if process.returncode != 0:
        print(f"Branch '{BRANCH}' does not exist locally, Failed to delete local branch")
    print(f'Deleted local branch {BRANCH}')


def fetch_all():
    print("Fetching all branches from remote")
    process = Popen(f"git fetch --all", shell=True, stdout=PIPE)
    out, err = process.communicate()
    print("git log:", out, err)
    # process.wait()
    # if process.returncode != 0:
    #     print("Failed to take fetch all")
    #     raise Exception
    # print('Fetched all branches from remote successfully')


def switch_branch():
    print(f"Switching to {BRANCH}")
    process = Popen(f"git checkout {BRANCH}", shell=True, stdout=PIPE)
    process.wait()
    if process.returncode != 0:
        print(f"Failed to switch to branch {BRANCH}")
        raise Exception
    print(process.stdout.read())
    print(f'Branch switched to {BRANCH}')


def take_latest_pull():
    print("Taking latest pull for current local branch")
    process = Popen(f"git pull", shell=True, stdout=PIPE)
    out, err = process.communicate()
    print("git pull:", out, err)
    # process.wait()
    # if process.returncode != 0:
    #     print("Failed to take latest pull")
    #     raise Exception
    # print("Latest pull for current local branch taken")

def set_git_credentials():
    process = Popen(f"git config --global user.email '{username}@retrocausal.ai'", shell=True, stdout=PIPE)
    out, err = process.communicate()
    print("git pull:", out, err)
    process = Popen(f"git config --global user.username {username}", shell=True, stdout=PIPE)
    out, err = process.communicate()
    print("git pull:", out, err)
    process = Popen(f"git config --global user.password rqjpocvkad2wmjnax3icjgxwckpw4tnyqnvo33jteitdb7fi2z6q", shell=True, stdout=PIPE)
    out, err = process.communicate()
    print("git pull:", out, err)
    process = Popen(f'git config --list', shell=True, stdout=PIPE)
    out, err = process.communicate()
    print("git pull:", out, err)


def perform_stash():
    process = Popen(f"git stash", shell=True, stdout=PIPE)
    out, err = process.communicate()
    print("git stash:", out, err)

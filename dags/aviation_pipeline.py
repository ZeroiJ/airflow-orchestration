import os
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# Define alerting callback for task failure
def slack_alert_callback(context):
    webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
    if not webhook_url:
        print("SLACK_WEBHOOK_URL not configured. Skipping alert.")
        return
        
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    exec_date = context.get('execution_date')
    log_url = task_instance.log_url

    slack_msg = {
        "text": f":red_circle: Task Failed.\n"
                f"*Task*: {task_id}\n"
                f"*DAG*: {dag_id}\n"
                f"*Execution Time*: {exec_date}\n"
                f"*Log URL*: {log_url}"
    }
    
    try:
        response = requests.post(webhook_url, json=slack_msg)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to send Slack alert: {e}")

# Define alerting callback for SLA misses
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
    if not webhook_url:
        return
        
    slack_msg = {
        "text": f":warning: SLA Missed for DAG {dag.dag_id}!\n"
                f"Tasks that missed SLA: {[task.task_id for task in task_list]}"
    }
    try:
        requests.post(webhook_url, json=slack_msg)
    except Exception as e:
        print(f"Failed to send Slack alert: {e}")

# Default args with retries, exponential backoff, and SLA
default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack_alert_callback,
    'sla': timedelta(minutes=30),  # SLA check for tasks
}

with DAG(
    'aviation_pipeline',
    default_args=default_args,
    description='Aviation data ingestion and dbt transformation pipeline',
    schedule_interval='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=True,  # Enable backfill support
    tags=['aviation', 'dbt'],
    sla_miss_callback=sla_miss_callback,
) as dag:

    # Path setup
    ROADMAP_DIR = '/home/zeroij/aviation_de_roadmap'
    INGESTION_DIR = os.path.join(ROADMAP_DIR, 'flight-ingestion-pipeline')
    DBT_DIR = os.path.join(ROADMAP_DIR, 'flight_dbt')
    
    INGEST_VENV_PYTHON = os.path.join(INGESTION_DIR, '.venv', 'bin', 'python')
    DBT_VENV_BIN = os.path.join(ROADMAP_DIR, 'dbt-venv', 'bin', 'dbt')

    # Task 1: Run scheduled flight ingestion
    run_ingestion = BashOperator(
        task_id='run_ingestion',
        bash_command=f'cd {INGESTION_DIR} && {INGEST_VENV_PYTHON} main.py',
    )

    # Task 2: Run dbt models (transformation)
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command=f'cd {DBT_DIR} && {DBT_VENV_BIN} run',
    )

    # Task 3: Run dbt tests (data quality)
    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command=f'cd {DBT_DIR} && {DBT_VENV_BIN} test',
    )

    # Define task dependencies
    run_ingestion >> run_dbt_models >> run_dbt_tests

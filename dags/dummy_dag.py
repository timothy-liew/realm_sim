from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "Timothy",
    "start_date": datetime(2025, 4, 18),
}

with DAG(
    dag_id="dummy_simulation_dag",
    default_args=default_args,
    schedule_interval=None,# Manual trigger for now
    catchup=False,
    tags=['test', 'simulate']
):

    simulate = BashOperator(
        task_id="run_dummy_simulation",
        bash_command="python /opt/airflow/src/simulate/dummy_simulation.py"
    )
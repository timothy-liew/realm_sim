from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Timothy'
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='battle_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2025,4,16),
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    simulate = BashOperator(
        task_id='simulate_battles',
        bash_command='python /opt/airflow/src/simulate/battle_simulator.py'
    )

    transform = BashOperator(
        task_id='transform_battles',
        bash_command='python /opt/airflow/src/etl/transform.py'
    )

    simulate >> transform
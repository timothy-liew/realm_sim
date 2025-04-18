from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Timothy',
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id='battle_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,# Manual trigger for now
    catchup=False,
    tags=["spark", "simulate"]
) as dag:

    simulate = SparkSubmitOperator(
        task_id="simulate_battles",
        application="/opt/airflow/src/simulate/battle_simulator.py",
        conn_id="spark_default",
        total_executor_cores=1,
        executor_memory='2g',
        num_executors=1,
        driver_memory='2g',
        verbose=True
    )

    # simulate = BashOperator(
    #     task_id='simulate_battles',
    #     bash_command='python /opt/airflow/src/simulate/battle_simulator.py'
    # )

    # transform = BashOperator(
    #     task_id='transform_battles',
    #     bash_command='python /opt/airflow/src/etl/transform.py'
    # )
    #
    # simulate >> transform
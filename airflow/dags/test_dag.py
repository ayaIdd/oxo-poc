from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "test_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["test"],
) as dag:
    
    task1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )
    
    task2 = BashOperator(
        task_id="echo_hello",
        bash_command="echo 'Hello Airflow!'",
    )
    
    task1 >> task2

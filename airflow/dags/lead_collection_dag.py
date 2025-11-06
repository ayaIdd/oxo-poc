from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='lead_collection_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    label = BashOperator(
        task_id='label_data',
        bash_command='cd /home/aya/oxo-poc && python scripts/label.py'
    )

    train = BashOperator(
        task_id='train_model',
        bash_command='cd /home/aya/oxo-poc && python scripts/train.py'
    )

    score = BashOperator(
        task_id='score_leads', 
        bash_command='cd /home/aya/oxo-poc && python scripts/score.py'
    )

    label >> train >> score
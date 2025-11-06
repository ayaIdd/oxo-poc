from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='lead_collection_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',  # Runs every day
    catchup=False
) as dag:

    # generate = BashOperator(
    #     task_id='generate_synthetic',
    #     bash_command='python /path/to/scripts/1_generate_synthetic_data.py'
    # )
    
    # clean = BashOperator(
    #     task_id='clean_data',
    #     bash_command='python /path/to/scripts/2_clean_data.py'
    # )

    # enrich = BashOperator(
    #     task_id='enrich_features',
    #     bash_command='python /path/to/scripts/3_feature_engineering.py'
    # )

    label = BashOperator(
        task_id='label_data',
        bash_command='python scripts\label.py'
    )

    train = BashOperator(
        task_id='train_model',
        bash_command='python scripts\train.py'
    )

    score = BashOperator(
        task_id='score_leads',
        bash_command='python scripts\score.py'
    )

    # Define order

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd, json, requests

def collect_data():
    # your fake data generation
    pass

def clean_and_score():
    # your cleaning/scoring logic
    pass

def send_to_crm():
    df = pd.read_json('/tmp/qualified_leads.json')
    for _, lead in df.iterrows():
        payload = {
            "name": lead['restaurant_name'],
            "email_from": lead['email'],
            "phone": lead['phone'],
            "priority": lead['priority'],
        }
        print(f"Sent {lead['restaurant_name']} to CRM")

with DAG(
    dag_id='oxo_prospecting_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    collect = PythonOperator(task_id='collect_data', python_callable=collect_data)
    clean = PythonOperator(task_id='clean_and_score', python_callable=clean_and_score)
    sync = PythonOperator(task_id='send_to_crm', python_callable=send_to_crm)

    collect >> clean >> sync

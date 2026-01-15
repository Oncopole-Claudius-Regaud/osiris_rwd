from airflow import DAG
import sys
import os
from pathlib import Path
from airflow.operators.python import PythonOperator
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR / "loader"))

from loader.patient import load  

with DAG(
    dag_id="osrisis_rw_loader",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["osrisis", "load"]
) as dag:

    load_patient_task = PythonOperator(
        task_id="load_patient",
        python_callable=load
    )

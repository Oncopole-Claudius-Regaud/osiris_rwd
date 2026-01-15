from airflow import DAG
import sys
import os
from pathlib import Path
from airflow.operators.python import PythonOperator
from datetime import datetime
dag_folder = os.path.dirname(os.path.abspath(__file__))
if dag_folder not in sys.path:
    sys.path.insert(0, dag_folder)
loader_folder = os.path.join(dag_folder, "loader")
if loader_folder not in sys.path:
    sys.path.insert(0, loader_folder)
from patient import load

with DAG(
    dag_id="osrisis_rw_loader",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["osiris_rw", "load"]
) as dag:

    load_patient_task = PythonOperator(
        task_id="load_patient",
        python_callable=load
    )

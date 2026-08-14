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
from dataset import update_dataset
from lastnews import load_lastnews
from opposition import load_opposition
from relatedpathology import load_relatedpathology
from riskfactor import load_riskfactor
from familycancerhistory import load_familycancerhistory

with DAG(
    dag_id="osrisis_rw_loader",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["osiris_rw", "load"]
) as dag:

    update_dataset_task = PythonOperator(
        task_id="update_dataset",
        python_callable=update_dataset
    )

    load_patient_task = PythonOperator(
        task_id="load_patient",
        python_callable=load
    )
    load_lastnews_task = PythonOperator(
        task_id="load_lastnews",
        python_callable=load_lastnews
    )

    load_opposition_task = PythonOperator(
        task_id="load_opposition",
        python_callable=load_opposition
    )

    load_relatedpathology_task = PythonOperator(
        task_id="load_relatedpathology",
        python_callable=load_relatedpathology
    )

    load_riskfactor_task = PythonOperator(
        task_id="load_riskfactor",
        python_callable=load_riskfactor
    )

    load_familycancerhistory_task = PythonOperator(
        task_id="load_familycancerhistory",
        python_callable=load_familycancerhistory
    )

    update_dataset_task >> load_patient_task
    load_patient_task >> [
        load_lastnews_task,
        load_opposition_task,
        load_relatedpathology_task,
        load_riskfactor_task,
        load_familycancerhistory_task,
    ]

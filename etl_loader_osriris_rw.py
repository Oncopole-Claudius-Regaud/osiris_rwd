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
from primarycancer import load_primarycancer
from tnmevent import load_tnmevent
from primarycancerstage import load_primarycancerstage
from primarycancergrade import load_primarycancergrade
from tumorpathoevent import load_tumorpathoevent
from metastasis import load_metastasis
from analysis import load_analysis
from surgery import load_surgery
from progression import load_progression
from medication import load_medication
from measure import load_measure
from radiotherapy import load_radiotherapy

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

    load_primarycancer_task = PythonOperator(
        task_id="load_primarycancer",
        python_callable=load_primarycancer
    )

    load_tnmevent_task = PythonOperator(
        task_id="load_tnmevent",
        python_callable=load_tnmevent
    )

    load_primarycancerstage_task = PythonOperator(
        task_id="load_primarycancerstage",
        python_callable=load_primarycancerstage
    )

    load_primarycancergrade_task = PythonOperator(
        task_id="load_primarycancergrade",
        python_callable=load_primarycancergrade
    )

    load_tumorpathoevent_task = PythonOperator(
        task_id="load_tumorpathoevent",
        python_callable=load_tumorpathoevent
    )

    load_metastasis_task = PythonOperator(
        task_id="load_metastasis",
        python_callable=load_metastasis
    )

    load_analysis_task = PythonOperator(
        task_id="load_analysis",
        python_callable=load_analysis
    )

    load_surgery_task = PythonOperator(
        task_id="load_surgery",
        python_callable=load_surgery
    )

    load_progression_task = PythonOperator(
        task_id="load_progression",
        python_callable=load_progression
    )

    load_medication_task = PythonOperator(
        task_id="load_medication",
        python_callable=load_medication
    )

    load_measure_task = PythonOperator(
        task_id="load_measure",
        python_callable=load_measure
    )

    load_radiotherapy_task = PythonOperator(
        task_id="load_radiotherapy",
        python_callable=load_radiotherapy
    )

    update_dataset_task >> load_patient_task
    load_patient_task >> [
        load_lastnews_task,
        load_opposition_task,
        load_relatedpathology_task,
        load_riskfactor_task,
        load_familycancerhistory_task,
        load_primarycancer_task,
    ]
    load_primarycancer_task >> load_tnmevent_task
    load_primarycancer_task >> load_primarycancerstage_task
    load_primarycancer_task >> load_primarycancergrade_task
    load_primarycancer_task >> load_tumorpathoevent_task
    load_tumorpathoevent_task >> load_metastasis_task
    load_patient_task >> load_analysis_task
    load_patient_task >> load_surgery_task
    load_patient_task >> load_medication_task
    load_patient_task >> load_measure_task
    load_patient_task >> load_radiotherapy_task
    load_primarycancer_task >> load_progression_task

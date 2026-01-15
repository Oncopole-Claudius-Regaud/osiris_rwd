from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from loader.patient import load  # import direct de ta fonction

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

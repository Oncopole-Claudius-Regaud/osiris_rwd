from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import importlib

from config.datasets import DATASETS


def run_loader(dataset):
    module_path = DATASETS[dataset]["loader"]
    module_name, func_name = module_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    getattr(module, func_name)()


with DAG(
    dag_id="osrisis_rw_loader",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["osrisis", "load"]
) as dag:

    for dataset in DATASETS:
        PythonOperator(
            task_id=f"load_{dataset}",
            python_callable=run_loader,
            op_kwargs={"dataset": dataset}
        )

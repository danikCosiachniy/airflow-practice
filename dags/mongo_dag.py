import pandas as pd
import os
import glob
from datetime import datetime

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.datasets import Dataset

def get_fs_base_path(conn_id: str = "fs_default") -> str:
    """Достаём path из extra коннекта fs_default."""
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}
    base_path = extra.get("path")
    if not base_path:
        raise ValueError(f" Connection {conn_id} has no 'path' in extra")
    return base_path

def _getFilePath() -> str:
    """Функция на получение пути новго файла"""
    base_dir = get_fs_base_path("fs_default")   
    pattern = os.path.join(base_dir, "*.csv")
    files = glob.glob(pattern)
    if not files:
        return ""
    return files[0]

# Константа на путь к результирующему файлу
FILE_DATASET = Dataset(f"file://{_getFilePath()}")

with DAG(
    dag_id="mongo_loader_dag",
    start_date=datetime(2025, 11, 18),
    schedule=[FILE_DATASET],
    catchup=False,
    tags=["practice", "mongo"],
) as dag:
    
    def load_to_mongo():
        print(_getFilePath())
    
    read_data = PythonOperator(
            task_id='sort_by_date',
            python_callable=load_to_mongo,
        )
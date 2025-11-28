import pandas as pd
import os
import glob
from datetime import datetime
from dotenv import load_dotenv

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

def get_fs_base_path(conn_id: str = "fs_default") -> str:
    """Достаём path из extra коннекта fs_default."""
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}
    base_path = extra.get("path")
    if not base_path:
        raise ValueError(f" Connection {conn_id} has no 'path' in extra")
    return base_path



with DAG(
    dag_id="file_sensor_branch_dag1",
    start_date=datetime(2025, 11, 18),
    schedule_interval=None,          # запускаешь руками
    catchup=False,
    tags=["practice"],
) as dag:

    # 1. Сенсор: ждём появления файла
    wait_for_file = FileSensor(
        task_id="wait_for_file1",
        fs_conn_id="fs_default",
        filepath=".",
        poke_interval=10,           
        timeout=600,
        mode="poke",
    )

    # 2. Branch-таска: проверяем, пустой файл или нет
    @task.branch(task_id="check_file_is_empty")
    def check_file_is_empty() -> str:
        base_dir = get_fs_base_path("fs_default")   
        pattern = os.path.join(base_dir, "*.csv")
        files = glob.glob(pattern)

        if not files:
            # Нет вообще ни одного CSV — отдельная ветка
            return "no_csv_found"

        file_path = files[0]   # берём первый попавшийся, для учебной задачи норм
        size = os.path.getsize(file_path)

        if size > 0:
            return "process_file"
        else:
            return "empty_file"
    branch = check_file_is_empty()

    process_file = BashOperator(
        task_id="process_file",
        bash_command="echo 'File is NOT EMPTY, nothing to do'; sleep 5",
    )

    # 3. Ветвь, если файл пустой
    empty_file = BashOperator(
        task_id="empty_file",
        bash_command="echo 'File is EMPTY, nothing to do'; sleep 5",
    )

    # Задаём зависимости
    wait_for_file >> branch >> [process_file, empty_file]
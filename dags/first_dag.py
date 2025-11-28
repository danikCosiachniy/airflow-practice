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

def get_fs_base_path(conn_id: str = "fs_default") -> str:
    """Достаём path из extra коннекта fs_default."""
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}
    base_path = extra.get("path")
    if not base_path:
        raise ValueError(f" Connection {conn_id} has no 'path' in extra")
    return base_path

def _getFilePath()->str:
    """Функция на получение пути новго файла"""
    base_dir = get_fs_base_path("fs_default")   
    pattern = os.path.join(base_dir, "*.csv")
    files = glob.glob(pattern)
    if not files:
        return ""
    return files[0]

with DAG(
    dag_id="file_sensor_branch_dag",
    start_date=datetime(2025, 11, 18),
    schedule_interval=None,
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
        file_path = _getFilePath()
        size = os.path.getsize(file_path)

        if size > 0:
            return "data_processing.replace_nulls"
        else:
            return "empty_file"
    branch = check_file_is_empty()

   # 3. Ветвь, если файл пустой
    empty_file = BashOperator(
        task_id="empty_file",
        bash_command="echo 'File is EMPTY, nothing to do'; sleep 5",
    )

    # 4. Ветвь, если файл НЕ пустой — TaskGroup с обработкой данных
    with TaskGroup(group_id='data_processing') as data_processing:
        # Заменяем "null" на "-"
        def replace_nulls():
            """Заменить все значения 'null' на '-'"""
            df = pd.read_csv(_getFilePath())
            df = df.replace("null", "-")
        # Сортируем по created_date
        def sort_by_date():
            pass

        # Чистим content от эмодзи и лишних символов
        def clean_content():
            pass
        replace_nulls = PythonOperator(
            task_id="replace_nulls",
            python_callable=replace_nulls,
        )
        sort_by_date = PythonOperator(
            task_id='sort_by_date',
            python_callable=sort_by_date,
        )
        clean_content = PythonOperator(
            task_id='clean_data',
            python_callable=clean_content,
        )
        replace_nulls >> sort_by_date >> clean_content
    
    # Задаём зависимости
    wait_for_file >> branch >> [empty_file, data_processing]
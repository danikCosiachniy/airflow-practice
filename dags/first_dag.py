import pandas as pd
import os
import re
import glob
from datetime import datetime

from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

from utils.constants import FILE_DATASET
from utils.get_filepath import getFilePath

with DAG(
    dag_id="file_sensor_branch_dag",
    start_date=datetime(2025, 11, 18),
    schedule_interval=None,
    catchup=False,
    tags=["practice"],
) as dag:
    def file_appeared() -> bool:
        return bool(getFilePath())

    # 1. Сенсор: ждём появления файла
    wait_for_file = PythonSensor(
    task_id="wait_for_file1",
    python_callable=file_appeared,
    poke_interval=10,
    timeout=600,
    mode="reschedule",
)

    # 2. Branch-таска: проверяем, пустой файл или нет
    @task.branch(task_id="check_file_is_empty")
    def check_file_is_empty() -> str:
        """Функция на проверку пустой файл или нет"""
        file_path = getFilePath()
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
        def replace_nulls():
            """Заменить все значения 'null' на '-'"""
            # Читаем путь файла
            file_path = getFilePath()
            # Читаем CSV
            df = pd.read_csv(file_path)
            # Заменяем null на -
            df = df.replace("null", "-")
            # Записываем в тот же файл
            df.to_csv(file_path, index=False)
        
        def sort_by_date():
            """Сортируем по created_date"""
            # Читаем путь файла
            file_path = getFilePath()
            # Читаем файл и парсим столбец даты
            df = pd.read_csv(file_path, parse_dates=["at"])
            # Сортируем 
            df = df.sort_values("at")
            # Записываем результат в тот же файл 
            df.to_csv(file_path, index=False)
        
        def _clean_text(text):
            """Приватная функция на удаление части строки несоотвествующей регулярному выражению"""
            if not isinstance(text, str):
                return text
            return re.sub(r"[^\w\s.,!?;:()\"'-]", "", text)
        
        def clean_content():
            """Чистим content от эмодзи и лишних символов"""
            # Читаем путь к файлу
            file_path = getFilePath()
            # Читаем файл
            df = pd.read_csv(file_path)
            if "content" in df.columns:
                # Чистим колонку контента через функцию _clean_text
                df["content"] = df["content"].apply(_clean_text)
            # Записываем в тот же файл 
            df.to_csv(file_path, index=False)

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
            outlets=[FILE_DATASET]
        )
        # Задаем зависимоти внутри таскГруппы 
        replace_nulls >> sort_by_date >> clean_content
    
    # Задаем зависимости
    wait_for_file >> branch >> [empty_file, data_processing]
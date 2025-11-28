from datetime import datetime
import os

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.decorators import task


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
        fs_conn_id="fs_default",     # коннект к ФС из UI
        filepath="input_data/test.csv",       # относительно path из fs_default
        poke_interval=60,            # раз в 60 секунд
        timeout=600,                 # максимум 10 минут
        mode="poke",                 # или "reschedule"
    )

    # 2. Branch-таска: проверяем, пустой файл или нет
    @task.branch(task_id="check_file_is_empty")
    def check_file_is_empty() -> str:
        # путь к файлу внутри контейнера
        file_path = "/opt/airflow/data/input_data/test.csv"

        if not os.path.exists(file_path):
            # чисто на всякий случай — если файл пропал
            raise FileNotFoundError(f"File not found: {file_path}")

        size = os.path.getsize(file_path)
        if size > 0:
            # говорим Airflow идти в таску process_file
            return "process_file"
        else:
            # если пустой — идём в таску empty_file
            return "empty_file"

    branch = check_file_is_empty()

    # 3. Ветвь, если файл НЕ пустой
    process_file = BashOperator(
        task_id="process_file",
        bash_command="echo 'File is NOT empty, processing...'; sleep 5",
    )

    # 4. Ветвь, если файл пустой
    empty_file = BashOperator(
        task_id="empty_file",
        bash_command="echo 'File is EMPTY, nothing to do'; sleep 5",
    )

    # Задаём зависимости
    wait_for_file >> branch >> [process_file, empty_file]
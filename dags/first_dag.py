import os
import re
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup

from utils.constants import FILE_DATASET
from utils.get_filepath import getFilePath

with DAG(
    dag_id='file_sensor_branch_dag',
    start_date=datetime(2025, 11, 18),
    schedule_interval=None,
    catchup=False,
    tags=['practice'],
) as dag:

    def file_appeared() -> bool:
        return bool(getFilePath())

    # 1. Sensor: wait for the data file to appear
    wait_for_file = PythonSensor(
        task_id='wait_for_file1',
        python_callable=file_appeared,
        poke_interval=10,
        timeout=600,
        mode='reschedule',
    )

    # 2. Branch task: check whether the file is empty
    @task.branch(task_id='check_file_is_empty')
    def check_file_is_empty() -> str:
        """Check whether the file is empty."""
        file_path = getFilePath()
        size = os.path.getsize(file_path)
        if size > 0:
            return 'data_processing.replace_nulls'
        return 'empty_file'

    branch = check_file_is_empty()

    # 3. Branch: file is empty
    empty_file = BashOperator(
        task_id='empty_file',
        bash_command="echo 'File is EMPTY, nothing to do'; sleep 5",
    )

    # 4. Branch: file has data — TaskGroup for transformations
    with TaskGroup(group_id='data_processing') as data_processing:

        def replace_nulls():
            """Replace all 'null' values with '-'"""
            # Get the file path
            file_path = getFilePath()
            # Read the CSV
            df = pd.read_csv(file_path)
            # Replace null markers with '-'
            df = df.replace('null', '-')
            # Write back to the same file
            df.to_csv(file_path, index=False)

        def sort_by_date():
            """Sort rows by the created date column."""
            # Get the file path
            file_path = getFilePath()
            # Read the file and parse the date column
            df = pd.read_csv(file_path, parse_dates=['at'])
            # Sort rows
            df = df.sort_values('at')
            # Write the result back to the same file
            df.to_csv(file_path, index=False)

        def _clean_text(text):
            """Helper to remove characters that do not match the allowed pattern."""
            if not isinstance(text, str):
                return text
            return re.sub(r"[^\w\s.,!?;:()\"'-]", '', text)

        def clean_content():
            """Clean the content column from emojis and unwanted symbols."""
            # Get the file path
            file_path = getFilePath()
            # Read the file
            df = pd.read_csv(file_path)
            if 'content' in df.columns:
                # Clean the content column using _clean_text
                df['content'] = df['content'].apply(_clean_text)
            # Write back to the same file
            df.to_csv(file_path, index=False)

        replace_nulls = PythonOperator(
            task_id='replace_nulls',
            python_callable=replace_nulls,
        )
        sort_by_date = PythonOperator(
            task_id='sort_by_date',
            python_callable=sort_by_date,
        )
        clean_content = PythonOperator(
            task_id='clean_data', python_callable=clean_content, outlets=[FILE_DATASET]
        )
        # Set dependencies inside the TaskGroup
        replace_nulls >> sort_by_date >> clean_content

    # Set dependencies
    wait_for_file >> branch >> [empty_file, data_processing]

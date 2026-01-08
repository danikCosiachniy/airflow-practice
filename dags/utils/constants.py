from airflow.datasets import Dataset

from utils.get_filepath import getFilePath

# Константа на путь к результирующему файлу
FILE_DATASET = Dataset(f"file://{getFilePath()}")

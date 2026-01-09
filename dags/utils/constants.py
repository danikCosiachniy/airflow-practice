from airflow.datasets import Dataset

from utils.get_filepath import getFilePath

# Constant on the path to the resulting file
FILE_DATASET = Dataset(f'file://{getFilePath()}')
# Constant of path to the output file
OUTPUT_DIR = '/opt/airflow/data/output_data'

import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo.errors import BulkWriteError

from utils.constants import FILE_DATASET
from utils.get_filepath import getFilePath


def load_to_mongo() -> None:
    """Load the processed CSV into MongoDB.

    Deduplication rule:
      - Only by `reviewId`.
        If `reviewId` is present, it is used as MongoDB `_id`.
        Duplicate `reviewId` values are ignored on insert.

    No other transformations are applied.
    """

    path = getFilePath()
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f'CSV not found: {path}')

    df = pd.read_csv(path)

    records = df.to_dict(orient='records')
    if not records:
        print('No records to insert into MongoDB')
        return

    # Use ONLY reviewId to deduplicate.
    docs: list[dict] = []
    for r in records:
        review_id = r.get('reviewId')
        if review_id is not None and str(review_id).strip() != '':
            r['_id'] = str(review_id)
        docs.append(r)

    hook = MongoHook(mongo_conn_id='mongo_default')
    collection = hook.get_collection(mongo_collection='processed_data')

    try:
        result = collection.insert_many(docs, ordered=False)
        print(f'Inserted {len(result.inserted_ids)} documents into MongoDB')
    except BulkWriteError as e:
        details = e.details or {}
        inserted = details.get('nInserted', 0)
        write_errors = details.get('writeErrors', []) or []
        dupes = sum(1 for we in write_errors if we.get('code') == 11000)
        other_errors = len(write_errors) - dupes

        print(
            'Mongo insert completed with errors. '
            f'inserted={inserted}, duplicates_ignored={dupes}, other_errors={other_errors}'
        )

        # If there were non-duplicate errors, fail the task.
        if other_errors:
            raise


with DAG(
    dag_id='mongo_dag',
    start_date=datetime(2025, 11, 18),
    schedule=[FILE_DATASET],
    catchup=False,
    tags=['practice', 'mongo'],
) as dag:
    load_data = PythonOperator(
        task_id='load_to_mongo',
        python_callable=load_to_mongo,
    )

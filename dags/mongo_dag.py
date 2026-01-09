import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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

    file_size_bytes = os.path.getsize(path)
    print(f'Using CSV: {path} (size_bytes={file_size_bytes})')

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

    # Insert in small batches to avoid MongoDB 16MB message limit.
    BATCH_SIZE = 1000

    total_inserted = 0
    total_dupes = 0

    for i in range(0, len(docs), BATCH_SIZE):
        batch = docs[i : i + BATCH_SIZE]
        try:
            res = collection.insert_many(batch, ordered=False)
            total_inserted += len(res.inserted_ids)
        except BulkWriteError as e:
            details = e.details or {}
            inserted = details.get('nInserted', 0)
            write_errors = details.get('writeErrors', []) or []
            dupes = sum(1 for we in write_errors if we.get('code') == 11000)
            other_errors = len(write_errors) - dupes

            total_inserted += inserted
            total_dupes += dupes

            # If there were non-duplicate errors, fail the task.
            if other_errors:
                print(
                    'Mongo insert batch failed with non-duplicate errors. '
                    f'batch_start={i}, inserted={inserted}, duplicates_ignored={dupes}, other_errors={other_errors}'
                )
                raise

    print(
        'Mongo load finished. '
        f'rows_read={len(docs)}, inserted={total_inserted}, duplicates_ignored={total_dupes}'
    )


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

    trigger_queries = TriggerDagRunOperator(
        task_id='trigger_mongo_queries',
        trigger_dag_id='mongo_queries_dag',
        # Make the triggered run id unique and traceable to the source run.
        trigger_run_id='triggered_by_mongo_dag__{{ run_id }}',
        conf={
            'source_dag_id': 'mongo_dag',
            'source_run_id': '{{ run_id }}',
            'source_logical_date': '{{ logical_date }}',
        },
        wait_for_completion=False,
    )

    load_data >> trigger_queries

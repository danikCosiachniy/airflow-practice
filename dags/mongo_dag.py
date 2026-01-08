import hashlib
import os
from datetime import datetime

import pandas as pd
from utils.get_filepath import getFilePath
from utils.constants import FILE_DATASET
from pymongo import UpdateOne

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

def _make_id(row: dict) -> str:
    """
    Чтобы не плодить дубликаты при повторном запуске:
    1) если в данных есть id/comment_id — используем
    2) иначе делаем sha1 по набору полей
    """
    for k in ("_id", "id", "comment_id"):
        v = row.get(k)
        if v is not None and str(v).strip() != "":
            return str(v)

    payload = f"{row.get('created_date','')}|{row.get('content','')}|{row.get('rating','')}"
    return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()


def load_to_mongo() -> None:
    path = getFilePath()
    if not os.path.exists(path):
        raise FileNotFoundError(f"Final CSV not found: {path}")

    # 1) читаем CSV
    df = pd.read_csv(path)

    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    records = df.to_dict(orient="records")
    if not records:
        print("No records to insert into MongoDB")
        return

    # 3) готовим документы + _id
    docs = []
    for r in records:
        r["_id"] = _make_id(r)

        # Timestamp -> python datetime (BSON Date)
        if isinstance(r.get("created_date"), pd.Timestamp):
            r["created_date"] = r["created_date"].to_pydatetime()

        docs.append(r)

    # 4) подключаемся к Mongo через Airflow Connection
    hook = MongoHook(mongo_conn_id="mongo_default")
    collection = hook.get_collection(mongo_collection="processed_data")

    # 5) upsert (без дублей)
    ops = [UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True) for d in docs]
    result = collection.bulk_write(ops, ordered=False)

    print(
        f"Mongo upsert done. "
        f"matched={result.matched_count}, modified={result.modified_count}, upserted={len(result.upserted_ids)}"
    )


with DAG(
    dag_id="mongo_dag",
    start_date=datetime(2025, 11, 18),
    schedule=[FILE_DATASET],
    catchup=False,
    tags=["practice", "mongo"],
) as dag:
    load_data = PythonOperator(
        task_id="load_to_mongo",
        python_callable=load_to_mongo,
    )
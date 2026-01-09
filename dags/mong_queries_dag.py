import json
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

from utils.constants import OUTPUT_DIR

def _ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def _write_json(path: str, data) -> None:
    _ensure_output_dir()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


def _write_jsonl(path: str, docs) -> None:
    _ensure_output_dir()
    with open(path, "w", encoding="utf-8") as f:
        for d in docs:
            f.write(json.dumps(d, ensure_ascii=False, default=str) + "\n")


def top_5_frequent_comments() -> None:
    """
    Top-5 most frequent 'content' values.
    """
    hook = MongoHook(mongo_conn_id="mongo_default")
    coll = hook.get_collection(mongo_collection="processed_data")

    pipeline = [
        {"$match": {"content": {"$type": "string", "$ne": ""}}},
        {"$group": {"_id": "$content", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5},
        {"$project": {"_id": 0, "content": "$_id", "count": 1}},
    ]

    result = list(coll.aggregate(pipeline))
    _write_json(os.path.join(OUTPUT_DIR, "top_5_frequent_comments.json"), result)


def content_less_than_5_chars() -> None:
    """
    All documents where content length < 5 characters.
    Writes JSONL because the result can be large.
    """
    hook = MongoHook(mongo_conn_id="mongo_default")
    coll = hook.get_collection(mongo_collection="processed_data")

    pipeline = [
        {"$match": {"content": {"$type": "string"}}},
        {
            "$match": {
                "$expr": {
                    "$lt": [{"$strLenCP": "$content"}, 5]
                }
            }
        },
        # Keep only useful fields to reduce output size
        {
            "$project": {
                "_id": 1,
                "reviewId": 1,
                "userName": 1,
                "content": 1,
                "score": 1,
                "at": 1,
            }
        },
    ]

    cursor = coll.aggregate(pipeline, allowDiskUse=True)
    _write_jsonl(os.path.join(OUTPUT_DIR, "content_len_lt_5.jsonl"), cursor)


def avg_rating_per_day() -> None:
    """
    Average score per day. The day key is a timestamp (ISODate at day start).
    """
    hook = MongoHook(mongo_conn_id="mongo_default")
    coll = hook.get_collection(mongo_collection="processed_data")

    pipeline = [
        {
            "$addFields": {
                "at_dt": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {"$eq": [{"$type": "$at"}, "date"]},
                                "then": "$at",
                            },
                            {
                                "case": {"$eq": [{"$type": "$at"}, "string"]},
                                "then": {
                                    "$dateFromString": {
                                        "dateString": "$at",
                                        "onError": None,
                                        "onNull": None,
                                    }
                                },
                            },
                        ],
                        "default": None,
                    }
                },
                "score_num": {
                    "$convert": {
                        "input": "$score",
                        "to": "double",
                        "onError": None,
                        "onNull": None,
                    }
                },
            }
        },
        {"$match": {"at_dt": {"$ne": None}, "score_num": {"$ne": None}}},
        {"$addFields": {"day": {"$dateTrunc": {"date": "$at_dt", "unit": "day"}}}},
        {"$group": {"_id": "$day", "avg_score": {"$avg": "$score_num"}}},
        {"$sort": {"_id": 1}},
        {"$project": {"_id": 0, "day_ts": "$_id", "avg_score": 1}},
    ]
    result = list(coll.aggregate(pipeline))
    _write_json(os.path.join(OUTPUT_DIR, "avg_score_per_day.json"), result)


with DAG(
    dag_id="mongo_queries_dag",
    start_date=datetime(2025, 11, 18),
    schedule=None,
    catchup=False,
    tags=["practice", "mongo", "queries"],
) as dag:
    t1 = PythonOperator(
        task_id="top_5_frequent_comments",
        python_callable=top_5_frequent_comments,
    )

    t2 = PythonOperator(
        task_id="content_len_lt_5",
        python_callable=content_less_than_5_chars,
    )

    t3 = PythonOperator(
        task_id="avg_score_per_day",
        python_callable=avg_rating_per_day,
    )

    # Run queries in parallel after this DAG is triggered
    [t1, t2, t3]
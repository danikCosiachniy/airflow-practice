import glob
import os
from airflow.hooks.base import BaseHook


def get_fs_base_path(conn_id: str = "fs_default") -> str:
    """Достаём path из extra коннекта fs_default."""
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}
    base_path = extra.get("path")
    if not base_path:
        raise ValueError(f" Connection {conn_id} has no 'path' in extra")
    return base_path

def getFilePath()->str:
    """Функция на получение пути новго файла"""
    base_dir = get_fs_base_path("fs_default")   
    pattern = os.path.join(base_dir, "*.csv")
    files = glob.glob(pattern)
    if not files:
        return ""
    return max(files, key=os.path.getmtime)

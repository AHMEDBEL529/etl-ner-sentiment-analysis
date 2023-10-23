# --------------- #
# PACKAGE IMPORTS #
# --------------- #

import logging
import os
import json
from minio import Minio
from airflow import Dataset
from pendulum import duration

# -------------------- #
# Enter your own info! #
# -------------------- #

MY_NAME = "AHMED"
MY_VIDEO_ID = "9jBgGY2Ww9Q"  # Add your desired YouTube video ID here

# ----------------------- #
# Configuration variables #
# ----------------------- #

# DAG default arguments
default_args = {
    "owner": "AHMED",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": duration(minutes=5),
}

# MinIO connection config
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_IP = "host.docker.internal:9000"
COMMENTS_BUCKET_NAME = "comments"  # Bucket for storing YouTube comments
ARCHIVE_BUCKET_NAME = "archive"

# DuckDB config
DUCKDB_INSTANCE_NAME = json.loads(os.environ["AIRFLOW_CONN_DUCKDB_DEFAULT"])["host"]
COMMENTS_IN_TABLE_NAME = "in_comments"

# Datasets
DS_DUCKDB_IN_COMMENTS = Dataset("duckdb://in_comments")
DS_COMMENTS_DATA_MINIO = Dataset(f"minio://{COMMENTS_BUCKET_NAME}")
DS_DUCKDB_REPORTING = Dataset("duckdb://reporting")
DS_START = Dataset("start")

# get Airflow task logger
task_log = logging.getLogger("airflow.task")

# utility functions
def get_minio_client():
    client = Minio(MINIO_IP, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    return client

# command to run streamlit app within codespaces/docker
# modifications are necessary to support double-port-forwarding
STREAMLIT_COMMAND = "streamlit run comments_app.py --server.enableWebsocketCompression=false --server.enableCORS=false"
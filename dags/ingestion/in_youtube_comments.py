"""DAG that queries and ingests YouTube comments from an API to MinIO."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from pendulum import datetime
import json

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv
from include.custom_task_groups.create_bucket import CreateBucket
from include.custom_operators.minio import LocalFilesystemToMinIOOperator
from include.youtube_utils import get_comments_from_youtube

# --- #
# DAG #
# --- #

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=[gv.DS_START],
    catchup=False,
    default_args=gv.default_args,
    description="Queries and ingests YouTube comments from an API to MinIO.",
    tags=["ingestion", "minio"],
    render_template_as_native_obj=True,
)
def ingest_youtube_comments():

    create_bucket_tg = CreateBucket(
        task_id="create_comments_bucket", bucket_name=gv.COMMENTS_BUCKET_NAME
    )

    @task
    def fetch_youtube_comments(video_id):
        comments_data = get_comments_from_youtube(video_id)
        return {"comments": comments_data}

    write_comments_to_minio = LocalFilesystemToMinIOOperator(
        task_id="write_comments_to_minio",
        bucket_name=gv.COMMENTS_BUCKET_NAME,
        object_name=f"{gv.MY_VIDEO_ID}.json",
        json_serializeable_information="{{ ti.xcom_pull(task_ids='fetch_youtube_comments') }}",
        outlets=[gv.DS_COMMENTS_DATA_MINIO],
    )

    comments_data = fetch_youtube_comments(gv.MY_VIDEO_ID)
    create_bucket_tg >> comments_data >> write_comments_to_minio

ingest_youtube_comments()
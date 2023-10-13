"""DAG that loads climate and weather data from MinIO to DuckDB."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from pendulum import datetime, parse
import duckdb
import os
import json

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv
from include.custom_task_groups.create_bucket import CreateBucket
from include.custom_operators.minio import (
    MinIOListOperator,
    MinIOCopyObjectOperator,
    MinIODeleteObjectsOperator,
)

# --- #
# DAG #
# --- #

@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the climate and weather data is ready in MinIO
    schedule=[gv.DS_COMMENTS_DATA_MINIO],
    catchup=False,
    default_args=gv.default_args,
    description="Loads climate and weather data from MinIO to DuckDB.",
    tags=["load", "minio", "duckdb"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)

def load_data():

    # create an instance of the CreateBucket task group consisting of 5 tasks
    # create_bucket_tg = CreateBucket(
    #     task_id="create_archive_bucket", bucket_name=gv.ARCHIVE_BUCKET_NAME
    # )

    list_files_comments_bucket = MinIOListOperator(
        task_id="list_files_comments_bucket", bucket_name=gv.COMMENTS_BUCKET_NAME
    )

    @task(outlets=[gv.DS_DUCKDB_IN_COMMENTS], pool="duckdb")
    def load_comments_data(obj):
        """Loads content of one fileobject in the MinIO comments bucket
        to DuckDB."""

        minio_client = gv.get_minio_client()
        # get the object from MinIO and save as a local tmp file
        minio_client.fget_object(gv.COMMENTS_BUCKET_NAME, obj, file_path=obj)

        cursor = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)

        # open the local tmp file to extract information
        with open(obj) as f:
            comments_data = json.load(f)
            print(comments_data)
            comments = comments_data["comments"]

        # write extracted information to DuckDB
        for comment in comments:
            # Ensure single quotes in the comment are escaped
            sanitized_comment = comment.replace("'", "''")
            cursor.execute(
                f"""
                    CREATE TABLE IF NOT EXISTS {gv.COMMENTS_IN_TABLE_NAME} (
                        TEXT VARCHAR(2550),
                    );
                    INSERT INTO {gv.COMMENTS_IN_TABLE_NAME} VALUES (
                        '{sanitized_comment}'
                    );"""
            )
        cursor.commit()
        cursor.close()

        # remove tmp json file
        os.remove(obj)

    # set dependencies

    # climate_data = load_climate_data.expand(obj=list_files_climate_bucket.output)
    weather_data = load_comments_data.expand(
        obj=list_files_comments_bucket.output
    )
    
    # # archive_bucket = create_bucket_tg

    # [climate_data, weather_data] >> archive_bucket
    # (archive_bucket >> [copy_objects_to_archive] >> delete_objects)


load_data()
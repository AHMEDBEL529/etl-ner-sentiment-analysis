"""DAG that loads climate and weather data from MinIO to DuckDB."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from pendulum import datetime
import duckdb
import os
import json

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv
from include.custom_task_groups.create_bucket import CreateBucket
from include.global_variables import global_variables as gv
from include.custom_operators.minio import (
    MinIOListOperator,
    MinIOCopyObjectOperator,
    MinIODeleteObjectsOperator,
)

# --- #
# DAG #
# --- #

def execute_main():
    from include.nlp_inference import main  # import the main function
    main()

@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the climate and weather data is ready in MinIO
    schedule=[gv.DS_DUCKDB_IN_COMMENTS],
    catchup=False,
    default_args=gv.default_args,
    description="Loads climate and weather data from MinIO to DuckDB.",
    tags=["load", "minio", "duckdb"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def transform():

    @task(outlets=[gv.DS_DUCKDB_REPORTING])
    def process_comments():
        execute_main()


    process_comments()

transform()  # Assign the DAG instance to a variable

import os
from datetime import datetime

import pandas as pd
from airflow.decorators import dag

from astro import sql as aql
from astro.files import File
from astro.table import Table

DUCKDB_CONN_ID = "duckdb_conn"
AWS_CONN_ID = "aws_default"
s3_bucket = os.getenv("S3_BUCKET", "s3://movement-sample/_output")

os.environ['AIRFLOW_CONN_DUCKDB_CONN'] = 'duckdb://%2Ftmp%2Fdb.duckdb'


@aql.transform()
def filter_data(input_table: Table):
    return """SELECT * FROM {{input_table}}"""


@aql.dataframe()
def transform_data(df: pd.DataFrame):
    # Faux transformation
    return df


@dag(
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
)
def sample_duckdb_etl():
    veraset_data = aql.load_file(
        input_file=File("s3://movement-sample/movement_dataset_metadata.csv", conn_id=AWS_CONN_ID),
        task_id="veraset_data",
        output_table=Table(conn_id=DUCKDB_CONN_ID),
    )

    filtered_dataframe = filter_data(
        veraset_data,
    )

    aggregated_dataframe = transform_data(
        filtered_dataframe,
        output_table=Table(conn_id=DUCKDB_CONN_ID),
    )

    aql.export_to_file(
        task_id="save_file",
        input_data=aggregated_dataframe,
        output_file=File(
            path=f"{s3_bucket}/{{{{ task_instance_key_str }}}}/aggregated_data_duckdb.csv",
            conn_id=AWS_CONN_ID,
        ),
        if_exists="replace",
    )

    aql.cleanup() # delete created temporary tables


dag = sample_duckdb_etl()

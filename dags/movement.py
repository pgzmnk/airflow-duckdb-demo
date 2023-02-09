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




@aql.run_raw_sql(conn_id=DUCKDB_CONN_ID)
def load_data():
    return """
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_region='us-west-2';
        CREATE OR REPLACE TABLE movement AS 
            SELECT * FROM read_parquet('s3://movement-sample/movement_parquet/year=2021/month=01/day=01/*') 
            WHERE cve_zm='09.01'
            AND year='2021'
            AND month='01'
            LIMIT 1000;
        """

@aql.transform(conn_id=DUCKDB_CONN_ID)
def filter_data():
    return """SELECT * FROM movement LIMIT 10"""


@aql.dataframe()
def transform_data(df: pd.DataFrame):
    # Faux transformation
    print(df)
    return df


@dag(
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
)
def movement():

    loaded_dataframe = load_data()

    filtered_dataframe = filter_data()

    aggregated_dataframe = transform_data(
        filtered_dataframe,
        output_table=Table(conn_id=DUCKDB_CONN_ID),
    )

    loaded_dataframe >> filtered_dataframe

    aql.cleanup() # delete created temporary tables


dag = movement()

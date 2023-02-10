from datetime import datetime
import os
import sys

# Resolves: `duckdb.IOException: IO Error: Extension "h3-duckdb/build/release/h3.duckdb_extension"could not be loaded: 
# h3-duckdb/build/release/h3.duckdb_extension: undefined symbol: _ZTIN6duckdb18BaseScalarFunctionE``
# https://github.com/duckdb/duckdb/issues/3243#issuecomment-1080521541
sys.setdlopenflags(os.RTLD_GLOBAL | os.RTLD_NOW)

import duckdb
import pandas as pd

from airflow.decorators import dag

from astro import sql as aql
from astro.files import File
from astro.table import Table

DUCKDB_CONN_ID = "duckdb_conn"
AWS_CONN_ID = "aws_default"
s3_bucket = os.getenv("S3_BUCKET", "s3://movement-sample/_output")

# Sets the connection string for the DuckDB connection
os.environ['AIRFLOW_CONN_DUCKDB_CONN'] = 'duckdb://%2Ftmp%2Fdb.duckdb'


@aql.dataframe(conn_id=DUCKDB_CONN_ID)
def load_h3_extension():
    """ Loads the H3 extension into the DuckDB database. 
    Note: This should be run from a SQL task once the astro decorator exposes ability to set `allow_unsigned_extensions`.
    """ 
    con = duckdb.connect(database="/tmp/db.duckdb", config={"allow_unsigned_extensions": "true"}, read_only=False)
    con.execute("LOAD 'h3-duckdb/build/release/h3.duckdb_extension';")
    df = con.execute("SELECT h3_cell_to_parent(cast(586265647244115967 as ubigint), 1);").df()
    print(df)
    return df


# @aql.transform(conn_id=DUCKDB_CONN_ID)
# def validate_h3_extension():
#     return """SELECT h3_cell_to_parent(cast(586265647244115967 as ubigint), 1);
#     """


@aql.run_raw_sql(conn_id=DUCKDB_CONN_ID)
def load_data():
    """ Loads `httpfs` extension and reads partitioned parquet data from S3.
    """
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

    load_h3 = load_h3_extension()

    # validate_h3 = validate_h3_extension()

    load_dataframe = load_data()

    filter_dataframe = filter_data()

    aggregate_dataframe = transform_data(
        filter_dataframe,
        output_table=Table(conn_id=DUCKDB_CONN_ID),
    )

    load_h3 >>  load_dataframe >> filter_dataframe


    aql.cleanup() # delete created temporary tables


dag = movement()

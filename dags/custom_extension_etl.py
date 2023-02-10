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
os.environ["AIRFLOW_CONN_DUCKDB_CONN"] = "duckdb://%2Ftmp%2Fdb.duckdb"


@aql.dataframe(conn_id=DUCKDB_CONN_ID)
def load_custom_extension_funct():
    """Loads the H3 extension into the DuckDB database.
    Note: This should be run from a SQL task once the astro decorator exposes ability to set `allow_unsigned_extensions`.
    See: https://github.com/astronomer/airflow-provider-duckdb/issues/4
    """
    con = duckdb.connect(
        database="/tmp/db.duckdb",
        config={"allow_unsigned_extensions": "true"},
        read_only=False,
    )
    con.execute("LOAD 'h3-duckdb/build/release/h3.duckdb_extension';")

    # Validate the extension was loaded
    df = con.execute("SELECT h3_cell_to_parent(cast(586265647244115967 as ubigint), 1);").df()
    print(df)
    return df


@aql.run_raw_sql(conn_id=DUCKDB_CONN_ID)
def load_data():
    """Loads `httpfs` extension and reads partitioned parquet data from S3."""
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


@aql.transform(conn_id=DUCKDB_CONN_ID)
def failed_task_validate_custom_extension():
    """This task fails.
    Note: Needs `allow_unsigned_extensions` to be set to true before the DuckDB connection is created.
    """
    return """
    LOAD 'h3-duckdb/build/release/h3.duckdb_extension';
    SELECT h3_cell_to_parent(cast(586265647244115967 as ubigint), 1);
    """


@aql.dataframe()
def failed_task_transform_data(df: pd.DataFrame):
    """This task fails.
    Note: The DuckDB connection for the input (which has an empty config) has precedence, so the `allow_unsigned_extensions`
    config doesn't set and the custom library doesn't load.
    """
    # Faux transformation
    con = duckdb.connect(
        database="/tmp/db.duckdb",
        config={"allow_unsigned_extensions": "true"},
        read_only=False,
    )
    con.execute("LOAD 'h3-duckdb/build/release/h3.duckdb_extension';")

    # Validate the extension was loaded
    df_2 = con.execute(
        "SELECT h3_cell_to_parent(cast(586265647244115967 as ubigint), 1);"
    ).df()
    print(df_2)
    return df_2


@dag(
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
)
def custom_extension_etl():
    load_custom_extension = load_custom_extension_funct()

    validate_custom = failed_task_validate_custom_extension()

    load_dataframe = load_data()

    filter_dataframe = filter_data()

    transform_dataframe = failed_task_transform_data(
        filter_dataframe,
        output_table=Table(conn_id=DUCKDB_CONN_ID),
    )

    load_custom_extension >> load_dataframe >> filter_dataframe >> transform_dataframe >> validate_custom

    aql.cleanup()  # delete created temporary tables


dag = custom_extension_etl()

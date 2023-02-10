# Airflow DuckDB Demo

This repo shows the latest DuckDB support of the [Astro SDK](https://github.com/astronomer/astro-sdk) Python library. It contains a happy path DAG and an edge case DAG that loads a custom DuckDB library.

The issues mentioned in `Caveats` below are tracked and should be resolved soon.

## Getting started

Spin up Airflow with the following command.

Note: Startup might take long the first time because the Dockerfile installs a custom library. This can be remediated by introducing custom images instead.

```
airflow dev start
```

Visit [http://localhost:8080/](http://localhost:8080/) and log in with `admin`:`admin`.

## Caveats

### Concurrency

The Astro SDK library will soon support concurrent execution for DuckDB tasks. In the meantime, tasks that run simultaneously might show errors such as:

```
sqlalchemy.exc.OperationalError: (duckdb.IOException) IO Error: Could not set lock on file "/tmp/db.duckdb": Resource temporarily unavailable
```

### Custom configuration

The Astro SDK library will soon support setting global DuckDB config, which will resolve the issues mentioned in failed tasks of `custom_extension_etl.py`.

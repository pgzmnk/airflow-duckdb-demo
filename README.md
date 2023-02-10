# Airflow DuckDB Demo

## Getting started

```
airflow db init
```

Visit [http://localhost:8080/](http://localhost:8080/) and log in with `admin`:`admin`.

## Caveats

The Astro SDK library will soon support concurrent execution for DuckDB tasks. In the meantime, you might see errors such as:

```
sqlalchemy.exc.OperationalError: (duckdb.IOException) IO Error: Could not set lock on file "/tmp/db.duckdb": Resource temporarily unavailable
```

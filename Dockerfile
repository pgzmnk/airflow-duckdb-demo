FROM quay.io/astronomer/astro-runtime:7.2.0

# Install cmake
USER root
RUN apt-get update
RUN wget http://cmake.org/files/v3.25/cmake-3.25.0.tar.gz
RUN tar xf cmake-3.25.0.tar.gz
# RUN (cd cmake-3.25.0/ && ./configure && make -- -DCMAKE_USE_OPENSSL=OFF)
RUN (cd cmake-3.25.0 && ./bootstrap -- -DCMAKE_USE_OPENSSL=OFF && make && make install)

# Install extension
RUN (cd h3-duckdb && \
    git config --global --add safe.directory /usr/local/airflow/h3-duckdb && \
    git config --global --add safe.directory /usr/local/airflow/h3-duckdb/duckdb && \
    git config --global --add safe.directory /usr/local/airflow/h3-duckdb/h3 && \
    git submodule update --init && \
    make duckdb_release release)
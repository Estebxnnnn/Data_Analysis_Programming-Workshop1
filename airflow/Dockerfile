# Custom Airflow image for TC-Analog Monitor (Team 4)
# Base: official Apache Airflow 2.10.5 image, Python 3.12 (Debian 12 "bookworm").
FROM apache/airflow:2.10.5-python3.12

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12

# --- Workshop 3: Java runtime for PySpark (local mode runs a JVM inside the task process).
# OpenJDK 17 is the Debian 12 default and is supported by Spark 3.5 and 4.x.
USER root
RUN mkdir -p /usr/share/man/man1 \
 && apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/* \
 && ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/tcmon-java
ENV JAVA_HOME=/usr/lib/jvm/tcmon-java
USER airflow

COPY requirements-airflow.txt /requirements-airflow.txt

# Install the pipeline dependencies against the official constraints file, so that
# pip never upgrades a package Airflow itself depends on (pyspark is pinned there too).
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /requirements-airflow.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Fail the build early if Spark cannot start a session with this Java/Python pair.
RUN python -c "import pyspark; print('pyspark', pyspark.__version__)" && java -version

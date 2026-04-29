FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
      curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    pyspark==4.1.0 \
    psycopg2-binary \
    requests \
    apache-airflow-providers-http

ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0 pyspark-shell"

from datetime import datetime, timedelta
import json
import os
import requests

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor


CONNECT_URL = os.environ.get("CONNECT_URL", "http://connect:8083")
PG_USER = os.environ.get("PG_USER", "cdc_user")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "cdc_pass")
PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/project")

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.10.0"
)

SPARK_SUBMIT_PREFIX = (
    f"PROJECT_ROOT={PROJECT_ROOT} spark-submit "
    f"--packages {SPARK_PACKAGES} "
    f"{PROJECT_ROOT}/jobs/project3_jobs.py"
)


def register_connector():
    connector_name = "project3-cdc"

    connector_config = {
        "name": connector_name,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "plugin.name": "pgoutput",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": PG_USER,
            "database.password": PG_PASSWORD,
            "database.dbname": "sourcedb",
            "topic.prefix": "dbserver1",
            "schema.include.list": "public",
            "table.include.list": "public.customers,public.drivers",
            "slot.name": "project3_slot",
            "publication.name": "project3_pub",
            "publication.autocreate.mode": "filtered",
            "snapshot.mode": "initial",
            "tombstones.on.delete": "true",
            "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
            "schema.history.internal.kafka.topic": "schemahistory.project3",
            "decimal.handling.mode": "double"
        }
    }

    existing = requests.get(f"{CONNECT_URL}/connectors", timeout=20)
    existing.raise_for_status()
    existing_connectors = existing.json()

    if connector_name in existing_connectors:
        response = requests.put(
            f"{CONNECT_URL}/connectors/{connector_name}/config",
            headers={"Content-Type": "application/json"},
            data=json.dumps(connector_config["config"]),
            timeout=30,
        )
    else:
        response = requests.post(
            f"{CONNECT_URL}/connectors",
            headers={"Content-Type": "application/json"},
            data=json.dumps(connector_config),
            timeout=30,
        )

    response.raise_for_status()
    print(response.text)


default_args = {
    "owner": "project3",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="project3_lakehouse_pipeline",
    description="CDC + Taxi + Driver scorecard pipeline",
    start_date=datetime(2026, 4, 28),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["project3", "cdc", "taxi", "iceberg", "airflow"],
) as dag:

    register_connector_task = PythonOperator(
        task_id="register_connector",
        python_callable=register_connector,
    )

    connector_health = HttpSensor(
        task_id="connector_health",
        http_conn_id="debezium_http",
        endpoint="connectors/project3-cdc/status",
        poke_interval=15,
        timeout=180,
        mode="poke",
        response_check=lambda response: (
            response.json().get("connector", {}).get("state") == "RUNNING"
            and all(
                task.get("state") == "RUNNING"
                for task in response.json().get("tasks", [])
            )
        ),
    )

    bronze_cdc = BashOperator(
        task_id="bronze_cdc",
        bash_command=f"cd {PROJECT_ROOT} && {SPARK_SUBMIT_PREFIX} bronze_cdc",
    )

    silver_cdc = BashOperator(
        task_id="silver_cdc",
        bash_command=f"cd {PROJECT_ROOT} && {SPARK_SUBMIT_PREFIX} silver_cdc",
    )

    bronze_taxi = BashOperator(
        task_id="bronze_taxi",
        bash_command=f"cd {PROJECT_ROOT} && {SPARK_SUBMIT_PREFIX} bronze_taxi",
    )

    silver_taxi = BashOperator(
        task_id="silver_taxi",
        bash_command=f"cd {PROJECT_ROOT} && {SPARK_SUBMIT_PREFIX} silver_taxi",
    )

    gold_taxi = BashOperator(
        task_id="gold_taxi",
        bash_command=f"cd {PROJECT_ROOT} && {SPARK_SUBMIT_PREFIX} gold_taxi",
    )

    gold_driver_scorecard = BashOperator(
        task_id="gold_driver_scorecard",
        bash_command=f"cd {PROJECT_ROOT} && {SPARK_SUBMIT_PREFIX} gold_driver_scorecard",
    )

    validate = BashOperator(
        task_id="validate",
        bash_command=f"cd {PROJECT_ROOT} && {SPARK_SUBMIT_PREFIX} validate",
    )

    register_connector_task >> connector_health

    connector_health >> bronze_cdc >> silver_cdc
    connector_health >> bronze_taxi >> silver_taxi

    silver_taxi >> gold_taxi
    [silver_cdc, silver_taxi] >> gold_driver_scorecard

    [silver_cdc, gold_taxi, gold_driver_scorecard] >> validate

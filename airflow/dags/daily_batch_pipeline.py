"""
Daily Batch Pipeline DAG
Runs the Bronze → Silver → Gold Spark jobs in sequence.
Scheduled hourly — each run processes all available landing data.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="daily_batch_pipeline",
    description="Bronze → Silver → Gold Spark medallion pipeline",
    default_args=default_args,
    start_date=datetime(2026, 8, 31),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spark", "medallion", "batch"],
) as dag:

    # ── Bronze ────────────────────────────────────────────────────────────────
    bronze_submit = SparkKubernetesOperator(
        task_id="bronze_submit",
        namespace="spark",
        application_file="kubernetes/spark/bronze-job.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    bronze_sensor = SparkKubernetesSensor(
        task_id="bronze_sensor",
        namespace="spark",
        application_name="{{ task_instance.xcom_pull(task_ids='bronze_submit')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
        timeout=900,
        poke_interval=30,
    )

    # ── Silver ────────────────────────────────────────────────────────────────
    silver_submit = SparkKubernetesOperator(
        task_id="silver_submit",
        namespace="spark",
        application_file="kubernetes/spark/silver-job.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    silver_sensor = SparkKubernetesSensor(
        task_id="silver_sensor",
        namespace="spark",
        application_name="{{ task_instance.xcom_pull(task_ids='silver_submit')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
        timeout=900,
        poke_interval=30,
    )

    # ── Gold ──────────────────────────────────────────────────────────────────
    gold_submit = SparkKubernetesOperator(
        task_id="gold_submit",
        namespace="spark",
        application_file="kubernetes/spark/gold-job.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    gold_sensor = SparkKubernetesSensor(
        task_id="gold_sensor",
        namespace="spark",
        application_name="{{ task_instance.xcom_pull(task_ids='gold_submit')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
        timeout=900,
        poke_interval=30,
    )

    # ── Task order ────────────────────────────────────────────────────────────
    bronze_submit >> bronze_sensor >> \
    silver_submit >> silver_sensor >> \
    gold_submit   >> gold_sensor

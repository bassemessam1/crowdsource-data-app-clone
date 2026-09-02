"""
Daily Batch Pipeline DAG
Runs Bronze → Silver → Gold Spark jobs in sequence using inline specs.
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

REPO = "europe-west2-docker.pkg.dev/crowdsource-data-app-clone/crowdsource-data-app/pyspark:3.5.1"

def spark_spec(name, job_file, driver_memory="1g", executor_memory="2g"):
    """Build a SparkApplication spec dict for the given job."""
    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": name,
            "namespace": "spark",
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "sparkVersion": "3.5.1",
            "image": REPO,
            "imagePullPolicy": "Always",
            "mainApplicationFile": f"local:///opt/spark/jobs/{job_file}",
            "sparkConf": {
                "spark.app.landing": "gs://crowdsource-data-app-clone-landing",
                "spark.app.bronze":  "gs://crowdsource-data-app-clone-bronze",
                "spark.app.silver":  "gs://crowdsource-data-app-clone-silver",
                "spark.app.gold":    "gs://crowdsource-data-app-clone-gold",
                "spark.app.bq_project": "crowdsource-data-app-clone",
                "spark.app.bq_dataset": "crowdsource_data_app_gold",
                "spark.hadoop.google.cloud.auth.service.account.enable": "true",
                "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            },
            "driver": {
                "cores": 1,
                "memory": driver_memory,
                "serviceAccount": "ksa-spark",
                "labels": {"version": "3.5.1"},
            },
            "executor": {
                "cores": 2,
                "instances": 2,
                "memory": executor_memory,
                "labels": {"version": "3.5.1"},
            },
            "restartPolicy": {"type": "Never"},
        },
    }


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

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
        application=spark_spec(
            "bronze-measurements",
            "bronze_job.py",
            driver_memory="1g",
            executor_memory="2g",
        ),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    bronze_sensor = SparkKubernetesSensor(
        task_id="bronze_sensor",
        namespace="spark",
        application_name="bronze-measurements",
        kubernetes_conn_id="kubernetes_default",
        timeout=900,
        poke_interval=30,
    )

    # ── Silver ────────────────────────────────────────────────────────────────
    silver_submit = SparkKubernetesOperator(
        task_id="silver_submit",
        namespace="spark",
        application=spark_spec(
            "silver-measurements",
            "silver_job.py",
            driver_memory="2g",
            executor_memory="3g",
        ),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    silver_sensor = SparkKubernetesSensor(
        task_id="silver_sensor",
        namespace="spark",
        application_name="silver-measurements",
        kubernetes_conn_id="kubernetes_default",
        timeout=900,
        poke_interval=30,
    )

    # ── Gold ──────────────────────────────────────────────────────────────────
    gold_submit = SparkKubernetesOperator(
        task_id="gold_submit",
        namespace="spark",
        application=spark_spec(
            "gold-operator-metrics",
            "gold_job.py",
            driver_memory="2g",
            executor_memory="4g",
        ),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    gold_sensor = SparkKubernetesSensor(
        task_id="gold_sensor",
        namespace="spark",
        application_name="gold-operator-metrics",
        kubernetes_conn_id="kubernetes_default",
        timeout=900,
        poke_interval=30,
    )

    # ── Task order ────────────────────────────────────────────────────────────
    bronze_submit >> bronze_sensor >> \
    silver_submit >> silver_sensor >> \
    gold_submit   >> gold_sensor

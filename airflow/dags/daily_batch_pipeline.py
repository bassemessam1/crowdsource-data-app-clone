"""
Daily Batch Pipeline DAG
Uses kubernetes Python client to submit SparkApplication CRDs.
No kubectl binary needed — uses in-cluster config.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

IMAGE = "europe-west2-docker.pkg.dev/crowdsource-data-app-clone/crowdsource-data-app/pyspark:3.5.1"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SPARK_CONF_BASE = {
    "spark.app.landing": "gs://crowdsource-data-app-clone-landing",
    "spark.app.bronze":  "gs://crowdsource-data-app-clone-bronze",
    "spark.app.silver":  "gs://crowdsource-data-app-clone-silver",
    "spark.app.gold":    "gs://crowdsource-data-app-clone-gold",
    "spark.hadoop.google.cloud.auth.service.account.enable": "true",
    "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
}


def _delete_job(api, name):
    try:
        api.delete_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace="spark",
            plural="sparkapplications",
            name=name,
        )
        print(f"Deleted existing {name}")
        import time; time.sleep(10)
    except Exception as e:
        print(f"{name} not found or already deleted: {e}")


def _build_spec(name, job_file, driver_mem, exec_mem, extra_conf=None):
    conf = {**SPARK_CONF_BASE}
    if extra_conf:
        conf.update(extra_conf)
    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": name, "namespace": "spark"},
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "sparkVersion": "3.5.1",
            "image": IMAGE,
            "imagePullPolicy": "Always",
            "mainApplicationFile": f"local:///opt/spark/jobs/{job_file}",
            "sparkConf": conf,
            "driver": {
                "cores": 1,
                "memory": driver_mem,
                "serviceAccount": "ksa-spark",
                "labels": {"version": "3.5.1"},
            },
            "executor": {
                "cores": 2,
                "instances": 2,
                "memory": exec_mem,
                "labels": {"version": "3.5.1"},
            },
            "restartPolicy": {"type": "Never"},
        },
    }


def _submit_and_wait(name, job_file, driver_mem, exec_mem, extra_conf=None):
    import time
    from kubernetes import client, config
    config.load_incluster_config()
    api = client.CustomObjectsApi()

    # Delete any existing job first
    _delete_job(api, name)

    # Submit
    spec = _build_spec(name, job_file, driver_mem, exec_mem, extra_conf)
    api.create_namespaced_custom_object(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        namespace="spark",
        plural="sparkapplications",
        body=spec,
    )
    print(f"Submitted {name} — waiting for completion...")

    # Poll for completion
    timeout = 900
    interval = 30
    elapsed = 0
    while elapsed < timeout:
        time.sleep(interval)
        elapsed += interval
        try:
            obj = api.get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace="spark",
                plural="sparkapplications",
                name=name,
            )
            state = obj.get("status", {}).get(
                "applicationState", {}).get("state", "UNKNOWN")
            print(f"  {name}: {state} ({elapsed}s elapsed)")
            if state == "COMPLETED":
                print(f"  {name} completed successfully")
                return
            if state in ("FAILED", "SUBMISSION_FAILED", "FAILING"):
                raise Exception(f"{name} failed with state: {state}")
        except Exception as e:
            if any(s in str(e) for s in ["FAILED", "FAILING", "failed"]):
                raise
            print(f"  Status check error (retrying): {e}")

    raise Exception(f"{name} timed out after {timeout}s")


def delete_all_jobs():
    from kubernetes import client, config
    config.load_incluster_config()
    api = client.CustomObjectsApi()
    for name in ["bronze-measurements", "silver-measurements", "gold-operator-metrics"]:
        _delete_job(api, name)


def run_bronze():
    _submit_and_wait("bronze-measurements", "bronze_job.py", "1g", "2g")


def run_silver():
    _submit_and_wait("silver-measurements", "silver_job.py", "2g", "3g")


def run_gold():
    _submit_and_wait(
        "gold-operator-metrics", "gold_job.py", "2g", "4g",
        extra_conf={
            "spark.app.bq_project": "crowdsource-data-app-clone",
            "spark.app.bq_dataset": "crowdsource_data_app_gold",
        },
    )


with DAG(
    dag_id="daily_batch_pipeline",
    description="Bronze → Silver → Gold Spark medallion pipeline",
    default_args=default_args,
    start_date=datetime(2026, 9, 27),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spark", "medallion", "batch"],
) as dag:

    delete_old = PythonOperator(
        task_id="delete_old_jobs",
        python_callable=delete_all_jobs,
    )

    bronze = PythonOperator(
        task_id="bronze_job",
        python_callable=run_bronze,
        execution_timeout=timedelta(minutes=20),
    )

    silver = PythonOperator(
        task_id="silver_job",
        python_callable=run_silver,
        execution_timeout=timedelta(minutes=20),
    )

    gold = PythonOperator(
        task_id="gold_job",
        python_callable=run_gold,
        execution_timeout=timedelta(minutes=20),
    )

    delete_old >> bronze >> silver >> gold

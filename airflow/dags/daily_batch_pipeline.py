"""
Daily Batch Pipeline DAG
Triggers Bronze → Silver → Gold Spark jobs via BashOperator + kubectl.
No external provider imports — uses only Airflow core operators.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

IMAGE = "europe-west2-docker.pkg.dev/crowdsource-data-app-clone/crowdsource-data-app/pyspark:3.5.1"

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

    delete_old = BashOperator(
        task_id="delete_old_jobs",
        bash_command="""
            kubectl delete sparkapplication \
              bronze-measurements silver-measurements gold-operator-metrics \
              -n spark 2>/dev/null || true
            sleep 5
        """,
    )

    bronze = BashOperator(
        task_id="bronze_job",
        bash_command="""
            cat <<'YAML' | kubectl apply -f -
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: bronze-measurements
  namespace: spark
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  sparkVersion: "3.5.1"
  image: """ + IMAGE + """
  imagePullPolicy: Always
  mainApplicationFile: local:///opt/spark/jobs/bronze_job.py
  sparkConf:
    spark.app.landing: gs://crowdsource-data-app-clone-landing
    spark.app.bronze: gs://crowdsource-data-app-clone-bronze
    spark.hadoop.google.cloud.auth.service.account.enable: "true"
    spark.hadoop.fs.gs.impl: com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
  driver:
    cores: 1
    memory: 1g
    serviceAccount: ksa-spark
  executor:
    cores: 2
    instances: 2
    memory: 2g
  restartPolicy:
    type: Never
YAML
            kubectl wait sparkapplication/bronze-measurements \
              --for=jsonpath='{.status.applicationState.state}'=COMPLETED \
              --timeout=900s -n spark
        """,
    )

    silver = BashOperator(
        task_id="silver_job",
        bash_command="""
            cat <<'YAML' | kubectl apply -f -
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: silver-measurements
  namespace: spark
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  sparkVersion: "3.5.1"
  image: """ + IMAGE + """
  imagePullPolicy: Always
  mainApplicationFile: local:///opt/spark/jobs/silver_job.py
  sparkConf:
    spark.app.bronze: gs://crowdsource-data-app-clone-bronze
    spark.app.silver: gs://crowdsource-data-app-clone-silver
    spark.hadoop.google.cloud.auth.service.account.enable: "true"
    spark.hadoop.fs.gs.impl: com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
  driver:
    cores: 1
    memory: 2g
    serviceAccount: ksa-spark
  executor:
    cores: 2
    instances: 2
    memory: 3g
  restartPolicy:
    type: Never
YAML
            kubectl wait sparkapplication/silver-measurements \
              --for=jsonpath='{.status.applicationState.state}'=COMPLETED \
              --timeout=900s -n spark
        """,
    )

    gold = BashOperator(
        task_id="gold_job",
        bash_command="""
            cat <<'YAML' | kubectl apply -f -
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: gold-operator-metrics
  namespace: spark
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  sparkVersion: "3.5.1"
  image: """ + IMAGE + """
  imagePullPolicy: Always
  mainApplicationFile: local:///opt/spark/jobs/gold_job.py
  sparkConf:
    spark.app.silver: gs://crowdsource-data-app-clone-silver
    spark.app.gold: gs://crowdsource-data-app-clone-gold
    spark.app.bq_project: crowdsource-data-app-clone
    spark.app.bq_dataset: opensignal_gold
    spark.hadoop.google.cloud.auth.service.account.enable: "true"
    spark.hadoop.fs.gs.impl: com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
  driver:
    cores: 1
    memory: 2g
    serviceAccount: ksa-spark
  executor:
    cores: 2
    instances: 2
    memory: 4g
  restartPolicy:
    type: Never
YAML
            kubectl wait sparkapplication/gold-operator-metrics \
              --for=jsonpath='{.status.applicationState.state}'=COMPLETED \
              --timeout=900s -n spark
        """,
    )

    delete_old >> bronze >> silver >> gold

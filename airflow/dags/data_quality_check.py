"""
Data Quality Check DAG
Runs after each Gold job to verify BigQuery has been populated.
Fails loudly if operator_metrics table is empty.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

default_args = {
    "owner": "data-engineering",
    "retries": 0,
    "email_on_failure": False,
}

with DAG(
    dag_id="data_quality_check",
    description="Validates BigQuery gold table row counts after pipeline runs",
    default_args=default_args,
    start_date=datetime(2026, 8, 31),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["quality", "bigquery"],
) as dag:

    # Fails if result is 0 or empty
    check_operator_metrics = BigQueryCheckOperator(
        task_id="check_operator_metrics",
        sql="""
            SELECT COUNT(*)
            FROM crowdsource_data_app_gold.operator_metrics
            WHERE year = CAST(FORMAT_DATE('%Y', CURRENT_DATE()) AS STRING)
              AND month = CAST(FORMAT_DATE('%m', CURRENT_DATE()) AS STRING)
              AND day = CAST(FORMAT_DATE('%d', CURRENT_DATE()) AS STRING)
        """,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    check_per_operator = BigQueryCheckOperator(
        task_id="check_per_operator",
        sql="""
            SELECT COUNT(DISTINCT operator_name)
            FROM crowdsource_data_app_gold.operator_metrics
        """,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    check_operator_metrics >> check_per_operator

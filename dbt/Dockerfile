FROM python:3.11-slim

WORKDIR /dbt

# Pinned to match the version already confirmed working locally
# (dbt=1.12.5, bigquery adapter=1.12.1)
RUN pip install --no-cache-dir dbt-bigquery==1.12.1

COPY dbt_project.yml packages.yml ./
COPY models/ ./models/

# profiles.yml here is safe to bake in: no secrets, just project/dataset/method.
# Auth itself comes from the pod's Workload Identity (ksa-dbt), same as
# `method: oauth` works locally via `gcloud auth application-default login`.
COPY profiles.yml.example ./profiles.yml

ENV DBT_PROFILES_DIR=/dbt
ENV PYTHONUNBUFFERED=1

# Resolve dbt_utils at build time, not at pod startup — avoids a network
# dependency on every DAG run and keeps the image reproducible.
RUN dbt deps

# Combined run+test, per your call: one task, fails (non-zero exit) if either
# dbt run or dbt test fails — sh -c with && ensures that exit code propagates.
CMD ["sh", "-c", "dbt run && dbt test"]

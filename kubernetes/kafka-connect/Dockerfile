# Reconstructed from `docker history --no-trunc` against the live image in
# Artifact Registry (kafka-connect-gcs:7.6.0) — no original Dockerfile existed
# in the repo. Confirmed base image + the single customization layer exactly;
# everything else below the JMX exporter section reproduces what's already
# running in production.

FROM confluentinc/cp-kafka-connect:7.6.0

# Exact command confirmed via `docker history --no-trunc`
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-gcs:10.0.7

# ── Phase 08 addition: Prometheus JMX exporter ───────────────────────────────
# The exporter's rule config (what MBeans map to what metric names) is NOT
# baked in here — it's supplied at runtime via a mounted ConfigMap, same
# pattern already used for the Kafka brokers' kafka-metrics-config.yml. Only
# the javaagent JAR itself is baked into the image.
ARG JMX_EXPORTER_VERSION=1.0.1
USER root
RUN curl -sSL -o /usr/share/java/jmx_prometheus_javaagent.jar \
    "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar"
USER appuser
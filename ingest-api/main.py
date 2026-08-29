"""
Opensignal Ingest API
Receives measurement events, validates against Avro schema,
produces to Kafka raw-measurements topic.
"""
import os
import json
import time
import logging
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringSerializer,
)

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "crowdsource-data-app-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
)
SCHEMA_REGISTRY_URL = os.getenv(
    "SCHEMA_REGISTRY_URL",
    "http://schema-registry.kafka.svc.cluster.local:8081"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-measurements")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "raw-measurements-dlq")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger(__name__)

# ── Avro Schema ───────────────────────────────────────────────────────────────
MEASUREMENT_SCHEMA = json.dumps({
    "type": "record",
    "name": "Measurement",
    "namespace": "com.opensignal.ingest",
    "fields": [
        {"name": "device_id",       "type": "string"},
        {"name": "timestamp",       "type": "long"},
        {"name": "latitude",        "type": "double"},
        {"name": "longitude",       "type": "double"},
        {"name": "download_speed",  "type": "float"},
        {"name": "upload_speed",    "type": "float"},
        {"name": "latency_ms",      "type": "int"},
        {"name": "operator_mccmnc", "type": "string"},
        {"name": "network_type",    "type": {
            "type": "enum",
            "name": "NetworkType",
            "symbols": ["WIFI", "NETWORK_3G", "NETWORK_4G", "NETWORK_5G", "UNKNOWN"]
        }}
    ]
})

# ── Global state ──────────────────────────────────────────────────────────────
producer: Producer = None
avro_serializer: AvroSerializer = None
string_serializer = StringSerializer("utf_8")


# ── Pydantic model ────────────────────────────────────────────────────────────
class Measurement(BaseModel):
    device_id:       str   = Field(..., min_length=1, max_length=64)
    timestamp:       int   = Field(..., gt=0)
    latitude:        float = Field(..., ge=-90.0,  le=90.0)
    longitude:       float = Field(..., ge=-180.0, le=180.0)
    download_speed:  float = Field(..., ge=0.0)
    upload_speed:    float = Field(..., ge=0.0)
    latency_ms:      int   = Field(..., ge=0)
    operator_mccmnc: str   = Field(..., min_length=4, max_length=6)
    network_type:    Literal["WIFI", "NETWORK_3G", "NETWORK_4G",
                             "NETWORK_5G", "UNKNOWN"]

    @field_validator("timestamp")
    @classmethod
    def timestamp_reasonable(cls, v):
        now_ms = int(time.time() * 1000)
        # Reject timestamps more than 1 hour in future or 7 days in past
        if v > now_ms + 3_600_000:
            raise ValueError("timestamp too far in future")
        if v < now_ms - 604_800_000:
            raise ValueError("timestamp too far in past")
        return v


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, avro_serializer

    log.info("Connecting to Schema Registry at %s", SCHEMA_REGISTRY_URL)
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    avro_serializer = AvroSerializer(
        schema_registry,
        MEASUREMENT_SCHEMA,
        lambda obj, ctx: obj,
    )

    log.info("Connecting to Kafka at %s", KAFKA_BOOTSTRAP)
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "retries": 3,
        "retry.backoff.ms": 300,
        "linger.ms": 5,
        "batch.size": 16384,
    })

    log.info("Ingest API ready")
    yield

    log.info("Flushing Kafka producer...")
    producer.flush(timeout=10)
    log.info("Shutdown complete")


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Opensignal Ingest API",
    description="Receives mobile network measurements and produces to Kafka",
    version="1.0.0",
    lifespan=lifespan,
)


def delivery_callback(err, msg):
    if err:
        log.error("Delivery failed: %s | topic=%s partition=%s",
                  err, msg.topic(), msg.partition())
    else:
        log.debug("Delivered to %s [%s] offset %s",
                  msg.topic(), msg.partition(), msg.offset())


@app.get("/health")
async def health():
    return {"status": "ok", "kafka": KAFKA_BOOTSTRAP}


@app.get("/")
async def root():
    return {"service": "ingest-api", "version": "1.0.0"}


@app.post("/measurements", status_code=200)
async def ingest_measurement(measurement: Measurement, request: Request):
    data = measurement.model_dump()

    try:
        # Serialise to Avro
        ctx = SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
        avro_bytes = avro_serializer(data, ctx)

        # Produce to Kafka — key by device_id for partition routing
        key_bytes = string_serializer(measurement.device_id)
        producer.produce(
            topic=KAFKA_TOPIC,
            key=key_bytes,
            value=avro_bytes,
            callback=delivery_callback,
        )
        producer.poll(0)  # Trigger delivery callbacks

        return {"status": "accepted", "device_id": measurement.device_id}

    except Exception as e:
        log.error("Failed to produce message: %s", e)

        # Send to DLQ as JSON with error context
        try:
            dlq_payload = json.dumps({
                "error": str(e),
                "original": data
            }).encode("utf-8")
            producer.produce(
                topic=DLQ_TOPIC,
                value=dlq_payload,
            )
            producer.poll(0)
        except Exception as dlq_err:
            log.error("DLQ produce also failed: %s", dlq_err)

        raise HTTPException(
            status_code=500,
            detail=f"Failed to publish event: {str(e)}"
        )


@app.exception_handler(422)
async def validation_exception_handler(request: Request, exc):
    log.warning("Validation error from %s: %s",
                request.client.host, exc.errors())
    return JSONResponse(
        status_code=422,
        content={"status": "invalid", "errors": exc.errors()}
    )

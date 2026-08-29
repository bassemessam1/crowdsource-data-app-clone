"""
Crowdsource Data App Measurement Simulator
Generates synthetic mobile network measurements and sends them to the Ingest API.
"""
import os
import time
import random
import json
import logging
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ── Configuration ─────────────────────────────────────────────────────────────
INGEST_URL = os.getenv("INGEST_API_URL", "http://ingest-api:8000/measurements")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "2"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

# ── UK Mobile Operators (MCC=234) ─────────────────────────────────────────────
OPERATORS = [
    {"mccmnc": "23430", "name": "EE"},
    {"mccmnc": "23420", "name": "Three"},
    {"mccmnc": "23410", "name": "O2"},
    {"mccmnc": "23415", "name": "Vodafone"},
]

NETWORK_TYPES = ["WIFI", "NETWORK_3G", "NETWORK_4G", "NETWORK_5G"]
NETWORK_WEIGHTS = [0.15, 0.05, 0.50, 0.30]

# ── UK bounding box ───────────────────────────────────────────────────────────
LAT_MIN, LAT_MAX = 50.0, 58.7
LON_MIN, LON_MAX = -5.5, 1.8

# ── Simulated device pool ─────────────────────────────────────────────────────
DEVICE_COUNT = 100
DEVICES = [f"device-{i:04d}" for i in range(DEVICE_COUNT)]


def generate_measurement() -> dict:
    operator = random.choice(OPERATORS)
    network_type = random.choices(NETWORK_TYPES, weights=NETWORK_WEIGHTS)[0]

    # Realistic speed ranges per network type
    speed_ranges = {
        "WIFI":       (10.0, 500.0, 5.0, 200.0),
        "NETWORK_5G": (50.0, 1000.0, 20.0, 100.0),
        "NETWORK_4G": (5.0, 150.0, 2.0, 50.0),
        "NETWORK_3G": (0.5, 10.0, 0.1, 5.0),
    }
    dl_min, dl_max, ul_min, ul_max = speed_ranges[network_type]

    latency_ranges = {
        "WIFI": (5, 50),
        "NETWORK_5G": (10, 30),
        "NETWORK_4G": (30, 80),
        "NETWORK_3G": (80, 300),
    }
    lat_min, lat_max = latency_ranges[network_type]

    return {
        "device_id": random.choice(DEVICES),
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "latitude": round(random.uniform(LAT_MIN, LAT_MAX), 6),
        "longitude": round(random.uniform(LON_MIN, LON_MAX), 6),
        "download_speed": round(random.uniform(dl_min, dl_max), 2),
        "upload_speed": round(random.uniform(ul_min, ul_max), 2),
        "latency_ms": random.randint(lat_min, lat_max),
        "operator_mccmnc": operator["mccmnc"],
        "network_type": network_type,
    }


def send_measurement(measurement: dict) -> bool:
    payload = json.dumps(measurement).encode("utf-8")
    req = urllib.request.Request(
        INGEST_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.status == 200:
                return True
            log.warning("Unexpected status: %d", resp.status)
            return False
    except urllib.error.HTTPError as e:
        log.error("HTTP error %d: %s", e.code, e.reason)
        return False
    except urllib.error.URLError as e:
        log.error("Connection error: %s", e.reason)
        return False
    except Exception as e:
        log.error("Unexpected error: %s", e)
        return False


def main():
    interval = 1.0 / EVENTS_PER_SECOND
    sent = 0
    failed = 0

    log.info("Simulator starting")
    log.info("Target: %s", INGEST_URL)
    log.info("Rate: %.1f events/second", EVENTS_PER_SECOND)
    log.info("Device pool: %d devices", DEVICE_COUNT)

    while True:
        start = time.monotonic()
        measurement = generate_measurement()

        if send_measurement(measurement):
            sent += 1
            if sent % 50 == 0:
                log.info(
                    "Sent %d events (failed: %d) | last: device=%s type=%s dl=%.1fMbps",
                    sent, failed,
                    measurement["device_id"],
                    measurement["network_type"],
                    measurement["download_speed"]
                )
        else:
            failed += 1
            if failed % 10 == 0:
                log.warning("Failed events: %d / %d total", failed, sent + failed)

        elapsed = time.monotonic() - start
        sleep_time = max(0, interval - elapsed)
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()

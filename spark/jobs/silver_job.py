"""
Silver Spark Job
Reads bronze Parquet, deduplicates, enriches with:
  - H3 geohash (resolution 7 ~1km)
  - Operator name from MCC/MNC lookup
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, row_number
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# MCC/MNC -> operator name lookup (UK operators)
OPERATOR_MAP = {
    "23430": "EE",
    "23420": "Three",
    "23410": "O2",
    "23415": "Vodafone",
    "23450": "O2",   # alternate MNC
    "23486": "EE",   # alternate MNC
}

def mccmnc_to_name(mccmnc):
    return OPERATOR_MAP.get(mccmnc, "Unknown")

def latlon_to_h3(lat, lon, resolution=7):
    """Convert lat/lon to H3 geohash index."""
    try:
        import h3
        return h3.latlng_to_cell(lat, lon, resolution)
    except Exception:
        return None

def main():
    spark = SparkSession.builder \
        .appName("silver-measurements") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.conf.get(
        "spark.app.bronze",
        "gs://crowdsource-data-app-clone-bronze"
    )
    silver = spark.conf.get(
        "spark.app.silver",
        "gs://crowdsource-data-app-clone-silver"
    )

    # UDFs
    operator_udf = udf(mccmnc_to_name, StringType())
    h3_udf = udf(lambda lat, lon: latlon_to_h3(lat, lon, 7), StringType())

    # Read bronze
    df = spark.read.parquet(f"{bronze}/measurements/")
    print(f"Bronze records: {df.count()}")

    # Deduplicate by device_id + timestamp (keep latest)
    window = Window \
        .partitionBy("device_id", "timestamp") \
        .orderBy(col("timestamp").desc())

    df = df \
        .withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    # Enrich
    df = df \
        .withColumn("operator_name", operator_udf(col("operator_mccmnc"))) \
        .withColumn("h3_index", h3_udf(col("latitude"), col("longitude")))

    print(f"Silver records after dedup: {df.count()}")

    # Write silver
    df.write \
        .format("parquet") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day", "operator_name") \
        .save(f"{silver}/measurements/")

    print("Silver job complete.")
    spark.stop()

if __name__ == "__main__":
    main()

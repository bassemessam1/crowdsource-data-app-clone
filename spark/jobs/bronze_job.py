"""
Bronze Spark Job
Reads raw Avro files from landing bucket, writes Parquet to bronze bucket.
No transformation — pure format conversion and partitioning.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, year, month, dayofmonth, hour

def main():
    spark = SparkSession.builder \
        .appName("bronze-measurements") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Config from environment or defaults
    landing = spark.conf.get(
        "spark.app.landing",
        "gs://crowdsource-data-app-clone-landing"
    )
    bronze = spark.conf.get(
        "spark.app.bronze",
        "gs://crowdsource-data-app-clone-bronze"
    )

    print(f"Reading from: {landing}")
    print(f"Writing to:  {bronze}")

    # Read all Avro files from landing
    df = spark.read \
        .format("avro") \
        .load(f"{landing}/topics/raw-measurements/")

    print(f"Records read: {df.count()}")

    # Add date partition columns from timestamp (epoch ms)
    df = df \
        .withColumn("event_ts", from_unixtime(col("timestamp") / 1000)) \
        .withColumn("year",  year("event_ts").cast("string")) \
        .withColumn("month", month("event_ts").cast("string")) \
        .withColumn("day",   dayofmonth("event_ts").cast("string")) \
        .withColumn("hour",  hour("event_ts").cast("string"))

    # Write Parquet partitioned by date
    df.write \
        .format("parquet") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day", "hour") \
        .save(f"{bronze}/measurements/")

    print("Bronze job complete.")
    spark.stop()

if __name__ == "__main__":
    main()
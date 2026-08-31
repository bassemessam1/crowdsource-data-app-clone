"""
Gold Spark Job
Aggregates silver data to operator-level metrics per hour.
Writes Parquet to gold bucket AND loads to BigQuery.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, percentile_approx, count, stddev
)

def main():
    spark = SparkSession.builder \
        .appName("gold-operator-metrics") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    silver = spark.conf.get(
        "spark.app.silver",
        "gs://crowdsource-data-app-clone-silver"
    )
    gold = spark.conf.get(
        "spark.app.gold",
        "gs://crowdsource-data-app-clone-gold"
    )
    bq_project = spark.conf.get(
        "spark.app.bq_project",
        "crowdsource-data-app-clone"
    )
    bq_dataset = spark.conf.get(
        "spark.app.bq_dataset",
        "crowdsource_data_app_gold"
    )

    df = spark.read.parquet(f"{silver}/measurements/")

    # Aggregate per operator per hour
    agg = df.groupBy(
        "operator_name", "year", "month", "day", "hour"
    ).agg(
        count("*").alias("measurement_count"),
        avg("download_speed").alias("avg_download_mbps"),
        avg("upload_speed").alias("avg_upload_mbps"),
        avg("latency_ms").alias("avg_latency_ms"),
        percentile_approx("download_speed", 0.5).alias("p50_download_mbps"),
        percentile_approx("download_speed", 0.9).alias("p90_download_mbps"),
        stddev("download_speed").alias("stddev_download_mbps"),
    )

    print(f"Gold rows: {agg.count()}")

    # Write to gold GCS
    agg.write \
        .format("parquet") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .save(f"{gold}/operator_metrics/")

    # Load to BigQuery
    agg.write \
        .format("bigquery") \
        .option("table", f"{bq_project}:{bq_dataset}.operator_metrics") \
        .option("temporaryGcsBucket", gold.replace("gs://", "")) \
        .mode("overwrite") \
        .save()

    print("Gold job complete.")
    spark.stop()

if __name__ == "__main__":
    main()

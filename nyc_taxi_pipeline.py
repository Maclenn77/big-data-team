import argparse
from typing import Iterable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _first_existing(columns: Iterable[str], candidates: Iterable[str]) -> Optional[str]:
    for name in candidates:
        if name in columns:
            return name
    return None


def _with_standard_timestamps(df: DataFrame) -> DataFrame:
    pickup_col = _first_existing(
        df.columns,
        ("pickup_datetime", "tpep_pickup_datetime", "lpep_pickup_datetime"),
    )
    dropoff_col = _first_existing(
        df.columns,
        ("dropoff_datetime", "tpep_dropoff_datetime", "lpep_dropoff_datetime"),
    )

    if pickup_col is None:
        raise ValueError("Input data must include a pickup datetime column.")

    df = df.withColumn("pickup_ts", F.to_timestamp(F.col(pickup_col)))
    if dropoff_col:
        df = df.withColumn("dropoff_ts", F.to_timestamp(F.col(dropoff_col)))

    return df.filter(F.col("pickup_ts").isNotNull())


def _write_trends(df: DataFrame, output_path: str) -> None:
    trends_path = f"{output_path}/trends"
    time_enriched = df.withColumn("pickup_hour", F.hour("pickup_ts")).withColumn(
        "pickup_date", F.to_date("pickup_ts")
    )

    trips_by_hour = (
        time_enriched
        .groupBy("pickup_hour")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("pickup_hour")
    )
    trips_by_hour.write.mode("overwrite").option("header", "true").csv(
        f"{trends_path}/trips_by_hour"
    )

    trips_by_day = (
        time_enriched
        .groupBy("pickup_date")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("pickup_date")
    )
    trips_by_day.write.mode("overwrite").option("header", "true").csv(
        f"{trends_path}/trips_by_day"
    )

    if "passenger_count" in df.columns:
        avg_passenger_by_hour = (
            time_enriched
            .groupBy("pickup_hour")
            .agg(F.avg("passenger_count").alias("avg_passenger_count"))
            .orderBy("pickup_hour")
        )
        avg_passenger_by_hour.write.mode("overwrite").option("header", "true").csv(
            f"{trends_path}/avg_passenger_by_hour"
        )

    if "fare_amount" in df.columns and "payment_type" in df.columns:
        revenue_by_payment = (
            df.groupBy("payment_type")
            .agg(F.sum("fare_amount").alias("total_fare_amount"))
            .orderBy("payment_type")
        )
        revenue_by_payment.write.mode("overwrite").option("header", "true").csv(
            f"{trends_path}/revenue_by_payment_type"
        )


def run_pipeline(input_path: str, output_path: str) -> None:
    spark = SparkSession.builder.appName("NYC Taxi Kaggle Pipeline").getOrCreate()
    try:
        # Keep schema inference enabled to support multiple Kaggle NYC taxi CSV formats.
        trips = spark.read.option("header", "true").option("inferSchema", "true").csv(
            input_path
        )
        standardized = _with_standard_timestamps(trips)

        standardized.write.mode("overwrite").parquet(f"{output_path}/cleaned_trips")
        _write_trends(standardized, output_path)
    finally:
        spark.stop()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process Kaggle NYC taxi data with PySpark and generate trend outputs."
    )
    parser.add_argument("--input", required=True, help="Path to input CSV file or folder")
    parser.add_argument("--output", required=True, help="Path to output folder")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_pipeline(args.input, args.output)

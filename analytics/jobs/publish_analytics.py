import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession


LAKE_ROOT = os.getenv("LAKE_ROOT", "hdfs://namenode:9000/railway/lake").rstrip("/")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://railway:railway_dev@postgres:5432/railway",
)


def rows_for(spark, path, columns):
    rows = spark.read.parquet(path).select(*columns).collect()
    return [
        tuple(None if value is None else value for value in row)
        for row in rows
    ]


def main():
    spark = (
        SparkSession.builder
        .appName("RailwayPublishGoldAnalytics")
        .config(
            "spark.hadoop.fs.defaultFS",
            os.getenv("HDFS_DEFAULT_FS", "hdfs://namenode:9000"),
        )
        .getOrCreate()
    )
    try:
        station_rows = rows_for(
            spark,
            f"{LAKE_ROOT}/gold/station_delay_summary",
            [
                "station_code", "station_name", "station_zone", "observation_count",
                "average_delay_minutes", "p90_delay_minutes", "delayed_fraction",
            ],
        )
        train_rows = rows_for(
            spark,
            f"{LAKE_ROOT}/gold/train_delay_summary",
            [
                "train_no", "train_name", "type_code", "observation_count",
                "average_delay_minutes", "p90_delay_minutes", "delayed_fraction",
            ],
        )
    finally:
        spark.stop()
    if not station_rows or not train_rows:
        raise RuntimeError("Gold summary datasets are empty; run prepare_data.py first")

    with psycopg2.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO station_delay_analytics
                    (station_code, station_name, zone, observation_count,
                     average_delay_minutes, p90_delay_minutes, delayed_fraction)
                VALUES %s
                ON CONFLICT (station_code) DO UPDATE SET
                    station_name = EXCLUDED.station_name,
                    zone = EXCLUDED.zone,
                    observation_count = EXCLUDED.observation_count,
                    average_delay_minutes = EXCLUDED.average_delay_minutes,
                    p90_delay_minutes = EXCLUDED.p90_delay_minutes,
                    delayed_fraction = EXCLUDED.delayed_fraction
                """,
                station_rows,
                page_size=1000,
            )
            execute_values(
                cursor,
                """
                INSERT INTO train_delay_analytics
                    (train_no, train_name, type_code, observation_count,
                     average_delay_minutes, p90_delay_minutes, delayed_fraction)
                VALUES %s
                ON CONFLICT (train_no) DO UPDATE SET
                    train_name = EXCLUDED.train_name,
                    type_code = EXCLUDED.type_code,
                    observation_count = EXCLUDED.observation_count,
                    average_delay_minutes = EXCLUDED.average_delay_minutes,
                    p90_delay_minutes = EXCLUDED.p90_delay_minutes,
                    delayed_fraction = EXCLUDED.delayed_fraction
                """,
                train_rows,
                page_size=1000,
            )
    print(f"Published {len(station_rows)} station and {len(train_rows)} train summaries")


if __name__ == "__main__":
    main()

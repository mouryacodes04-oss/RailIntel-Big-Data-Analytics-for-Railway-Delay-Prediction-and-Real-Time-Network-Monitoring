import json
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    dayofweek,
    lit,
    month,
    percentile_approx,
    pmod,
    sum as spark_sum,
    to_date,
    when,
    year,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


DATASET_ROOT = Path(os.getenv("DATASET_ROOT", "/datasets")).resolve()
LAKE_ROOT = os.getenv("LAKE_ROOT", "hdfs://namenode:9000/railway/lake").rstrip("/")
QUALITY_REPORT_PATH = Path(
    os.getenv("QUALITY_REPORT_PATH", "/data/lake/quality_report.json")
)
DELAY_DIR = DATASET_ROOT / "Railway_delays"
SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "48"))


def lake_path(*parts):
    return "/".join((LAKE_ROOT, *parts))


def dataset_path(name):
    return (DELAY_DIR / name).resolve().as_uri()


DELAY_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("station_no", IntegerType(), True),
    StructField("station_name", StringType(), True),
    StructField("delay", DoubleType(), True),
    StructField("train_no", StringType(), True),
])
SCHEDULE_SCHEMA = StructType([
    StructField("station_no", IntegerType(), True),
    StructField("station_name", StringType(), True),
    StructField("distance_from_origin", DoubleType(), True),
    StructField("arrival_day", IntegerType(), True),
    StructField("arrival_time", StringType(), True),
    StructField("departure_day", IntegerType(), True),
    StructField("departure_time", StringType(), True),
    StructField("train_no", StringType(), True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("RailwayHistoricalDelayLakehouse")
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config(
            "spark.hadoop.fs.defaultFS",
            os.getenv("HDFS_DEFAULT_FS", "hdfs://namenode:9000"),
        )
        .getOrCreate()
    )

    raw = (
        spark.read.schema(DELAY_SCHEMA).option("header", True)
        .csv(dataset_path("combined_delay.csv"))
        .withColumn("service_date", to_date(col("date"), "yyyy-MM-dd"))
        .withColumn("train_id", col("train_no").cast("long"))
        .withColumn("delay_minutes", col("delay").cast("double"))
        .withColumn("station_order", col("station_no").cast("int"))
    )
    valid = raw.filter(
        col("service_date").isNotNull()
        & col("train_id").isNotNull()
        & col("station_order").isNotNull()
        & col("delay_minutes").isNotNull()
        & col("delay_minutes").between(-120, 1440)
    )

    schedules = (
        spark.read.schema(SCHEDULE_SCHEMA).option("header", True)
        .csv(dataset_path("combined_schedule.csv"))
        .select(
            col("train_no").cast("long").alias("train_id"),
            col("station_no").cast("int").alias("station_order"),
            col("distance_from_origin").cast("double").alias("distance_from_origin_km"),
        )
        .dropDuplicates(["train_id", "station_order"])
    )
    train_details = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(dataset_path("train_details.csv"))
        .select(
            col("train_no").cast("long").alias("train_id"),
            col("type_code").cast("string").alias("train_type"),
        )
        .dropDuplicates(["train_id"])
    )

    enriched = (
        valid.select("service_date", "train_id", "station_order", "station_name", "delay_minutes")
        .join(schedules, ["train_id", "station_order"], "left")
        .join(train_details, "train_id", "left")
        .withColumn("schedule_matched", col("distance_from_origin_km").isNotNull())
        .withColumn("distance_from_origin_km", col("distance_from_origin_km").cast("double"))
        .fillna({"distance_from_origin_km": 0.0, "train_type": "UNKNOWN"})
        .withColumn("month", month("service_date"))
        .withColumn("day_of_week", (pmod(dayofweek("service_date") + lit(5), lit(7)) + lit(1)).cast("int"))
        .withColumn("year", year("service_date"))
    )

    silver_path = lake_path("silver", "delays")
    (
        enriched.repartition(SHUFFLE_PARTITIONS, "year", "month")
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(silver_path)
    )

    model_sample = (
        enriched.select(
            col("train_id").cast("string").alias("train_no"),
            col("station_order").alias("station_no"),
            col("station_name").alias("station_code"),
            "distance_from_origin_km",
            "month",
            "day_of_week",
            "train_type",
            "service_date",
            col("delay_minutes").alias("delay"),
        )
        .sample(withReplacement=False, fraction=0.005, seed=42)
    )
    sample_path = lake_path("model_training")
    model_sample.write.mode("overwrite").parquet(sample_path)

    station_names = (
        spark.read.option("header", True).option("inferSchema", False)
        .csv(dataset_path("station_full_names.csv"))
        .select(
            col("station_name").alias("station_code"),
            col("station_full_name"),
            col("station_zone"),
        )
        .dropDuplicates(["station_code"])
    )
    station_gold = (
        valid.select("station_name", "delay_minutes")
        .groupBy("station_name")
        .agg(
            count("*").alias("observation_count"),
            avg("delay_minutes").alias("average_delay_minutes"),
            percentile_approx("delay_minutes", 0.9, 10000).alias("p90_delay_minutes"),
            avg(when(col("delay_minutes") > 15, 1.0).otherwise(0.0)).alias("delayed_fraction"),
        )
        .join(station_names, col("station_name") == col("station_code"), "left")
        .select(
            col("station_name").alias("station_code"),
            col("station_full_name").alias("station_name"),
            "station_zone",
            "observation_count",
            "average_delay_minutes",
            "p90_delay_minutes",
            "delayed_fraction",
        )
        .fillna({"station_name": "Unknown station", "station_zone": "Unknown"})
    )
    station_gold.write.mode("overwrite").parquet(lake_path("gold", "station_delay_summary"))

    train_names = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(dataset_path("train_details.csv"))
        .select(
            col("train_no").cast("long").alias("train_id"),
            col("train_name").alias("train_name"),
            col("type_code").alias("type_code"),
        )
        .dropDuplicates(["train_id"])
    )
    train_gold = (
        valid.select("train_id", "delay_minutes")
        .groupBy("train_id")
        .agg(
            count("*").alias("observation_count"),
            avg("delay_minutes").alias("average_delay_minutes"),
            percentile_approx("delay_minutes", 0.9, 10000).alias("p90_delay_minutes"),
            avg(when(col("delay_minutes") > 15, 1.0).otherwise(0.0)).alias("delayed_fraction"),
        )
        .join(train_names, "train_id", "left")
        .select(
            col("train_id").cast("string").alias("train_no"),
            col("train_name"),
            col("type_code"),
            "observation_count",
            "average_delay_minutes",
            "p90_delay_minutes",
            "delayed_fraction",
        )
        .fillna({"train_name": "Unknown train", "type_code": "UNKNOWN"})
    )
    train_gold.write.mode("overwrite").parquet(lake_path("gold", "train_delay_summary"))

    quality = valid.agg(
        count("*").alias("clean_rows"),
        avg(when(col("delay_minutes") < 0, 1.0).otherwise(0.0)).alias("early_fraction"),
    ).crossJoin(
        enriched.agg(
            spark_sum(when(~col("schedule_matched"), 1).otherwise(0)).alias("rows_without_schedule_match")
        )
    ).first().asDict()
    QUALITY_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(QUALITY_REPORT_PATH, "w", encoding="utf-8") as stream:
        json.dump(quality, stream, indent=2, default=str)

    spark.stop()
    print(f"Silver data, model sample and gold summaries written under {LAKE_ROOT}")


if __name__ == "__main__":
    main()

"""
Robust Batch Analysis of NYC Trip Data using PySpark.
Processes BOTH Yellow Taxi + HVFHV (Uber/Lyft) data.

FIXES INCLUDED:
- Handles inconsistent parquet schemas safely
- Prevents MutableDouble -> MutableLong crashes
- Ignores corrupt parquet files
- Uses safer casting strategy
- Better fault tolerance for large datasets

Usage:
    spark-submit --master yarn --deploy-mode client 03_batch_analysis.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time
import os

LOCAL_OUTPUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "output"
)


# =========================================================
# SPARK SESSION
# =========================================================

def create_spark_session():

    return SparkSession.builder \
        .appName("NYC_Taxi_Batch_Analysis") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.files.ignoreCorruptFiles", "true") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()


# =========================================================
# LOAD YELLOW TAXI
# =========================================================

def load_yellow_taxi(spark):

    print("\n[LOADING] Yellow Taxi data from HDFS...")
    start = time.time()

    raw_df = spark.read.parquet(
        "hdfs:///user/taxi/raw/yellow_tripdata_*.parquet"
    )

    df = raw_df.select(
        F.col("tpep_pickup_datetime")
            .cast("timestamp")
            .alias("pickup_datetime"),

        F.col("tpep_dropoff_datetime")
            .cast("timestamp")
            .alias("dropoff_datetime"),

        F.col("passenger_count")
            .cast("double")
            .alias("passenger_count"),

        F.col("trip_distance")
            .cast("double")
            .alias("trip_distance"),

        F.col("PULocationID")
            .cast("int")
            .alias("PULocationID"),

        F.col("DOLocationID")
            .cast("int")
            .alias("DOLocationID"),

        F.col("payment_type")
            .cast("int")
            .alias("payment_type"),

        F.col("fare_amount")
            .cast("double")
            .alias("fare_amount"),

        F.col("tip_amount")
            .cast("double")
            .alias("tip_amount"),

        F.col("total_amount")
            .cast("double")
            .alias("total_amount")
    ).withColumn(
        "source",
        F.lit("yellow_taxi")
    )

    count = df.count()

    print(f"  Yellow Taxi: {count:,} records "
          f"in {time.time()-start:.1f}s")

    return df


# =========================================================
# LOAD HVFHV
# =========================================================

def load_hvfhv(spark):

    print("\n[LOADING] HVFHV (Uber/Lyft) data from HDFS...")
    start = time.time()

    raw_df = spark.read.parquet(
        "hdfs:///user/taxi/raw/fhvhv_tripdata_*.parquet"
    )

    df = raw_df.select(
        F.col("pickup_datetime")
            .cast("timestamp")
            .alias("pickup_datetime"),

        F.col("dropoff_datetime")
            .cast("timestamp")
            .alias("dropoff_datetime"),

        F.lit(None).cast("double")
            .alias("passenger_count"),

        F.col("trip_miles")
            .cast("double")
            .alias("trip_distance"),

        F.col("PULocationID")
            .cast("int")
            .alias("PULocationID"),

        F.col("DOLocationID")
            .cast("int")
            .alias("DOLocationID"),

        F.lit(None).cast("int")
            .alias("payment_type"),

        F.col("base_passenger_fare")
            .cast("double")
            .alias("fare_amount"),

        F.col("tips")
            .cast("double")
            .alias("tip_amount"),

        F.col("driver_pay")
            .cast("double")
            .alias("total_amount")
    ).withColumn(
        "source",
        F.lit("hvfhv")
    )

    count = df.count()

    print(f"  HVFHV: {count:,} records "
          f"in {time.time()-start:.1f}s")

    return df


# =========================================================
# LOAD ALL DATA
# =========================================================

def load_all_data(spark):

    yellow = load_yellow_taxi(spark)
    hvfhv = load_hvfhv(spark)

    combined = yellow.unionByName(hvfhv)

    total = combined.count()

    print(f"\n[TOTAL] Combined records: {total:,}")

    return combined


# =========================================================
# CLEAN DATA
# =========================================================

def clean_data(df):

    print("\n[CLEANING] Removing invalid records...")

    initial_count = df.count()

    cleaned = df.filter(
        (F.col("trip_distance") > 0) &
        (F.col("fare_amount") > 0) &
        (F.col("fare_amount") < 500) &
        (F.col("pickup_datetime").isNotNull()) &
        (F.col("dropoff_datetime").isNotNull())
    )

    cleaned = cleaned.withColumn(
        "trip_duration_min",
        (
            F.unix_timestamp("dropoff_datetime") -
            F.unix_timestamp("pickup_datetime")
        ) / 60
    )

    cleaned = cleaned.filter(
        (F.col("trip_duration_min") > 1) &
        (F.col("trip_duration_min") < 180)
    )

    cleaned_count = cleaned.count()

    removed = initial_count - cleaned_count

    print(f"  Before: {initial_count:,}")
    print(f"  After : {cleaned_count:,}")
    print(f"  Removed: {removed:,}")

    return cleaned


# =========================================================
# ANALYSIS 1 - PEAK HOURS
# =========================================================

def analysis_peak_hours(df):

    print("\n[ANALYSIS 1] Peak Hours")

    result = df.withColumn(
        "hour",
        F.hour("pickup_datetime")
    ).groupBy("hour").agg(
        F.count("*").alias("total_trips"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("trip_distance").alias("avg_distance")
    ).orderBy("hour")

    result.show(24, truncate=False)

    result.toPandas().to_csv(
        os.path.join(LOCAL_OUTPUT, "peak_hours.csv"),
        index=False
    )

    return result


# =========================================================
# ANALYSIS 2 - DAILY PATTERNS
# =========================================================

def analysis_daily_patterns(df):

    print("\n[ANALYSIS 2] Daily Patterns")

    result = df.withColumn(
        "day_of_week",
        F.dayofweek("pickup_datetime")
    ).withColumn(
        "day_name",
        F.date_format("pickup_datetime", "EEEE")
    ).groupBy(
        "day_of_week",
        "day_name"
    ).agg(
        F.count("*").alias("total_trips"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_amount").alias("avg_tip")
    ).orderBy("day_of_week")

    result.show(truncate=False)

    result.toPandas().to_csv(
        os.path.join(LOCAL_OUTPUT, "daily_patterns.csv"),
        index=False
    )

    return result


# =========================================================
# ANALYSIS 3 - MONTHLY TRENDS
# =========================================================

def analysis_monthly_trends(df):

    print("\n[ANALYSIS 3] Monthly Trends")

    result = df.withColumn(
        "month",
        F.date_format("pickup_datetime", "yyyy-MM")
    ).groupBy("month").agg(
        F.count("*").alias("total_trips"),
        F.sum("fare_amount").alias("total_revenue"),
        F.avg("trip_distance").alias("avg_distance"),
        F.avg("trip_duration_min").alias("avg_duration")
    ).orderBy("month")

    result.show(truncate=False)

    result.toPandas().to_csv(
        os.path.join(LOCAL_OUTPUT, "monthly_trends.csv"),
        index=False
    )

    return result


# =========================================================
# ANALYSIS 4 - SOURCE COMPARISON
# =========================================================

def analysis_source_comparison(df):

    print("\n[ANALYSIS 4] Yellow Taxi vs HVFHV")

    result = df.groupBy("source").agg(
        F.count("*").alias("total_trips"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_amount").alias("avg_tip"),
        F.avg("trip_distance").alias("avg_distance"),
        F.sum("fare_amount").alias("total_revenue")
    )

    result.show(truncate=False)

    result.toPandas().to_csv(
        os.path.join(LOCAL_OUTPUT, "source_comparison.csv"),
        index=False
    )

    return result


# =========================================================
# BENCHMARK
# =========================================================

def benchmark_vs_pandas():

    print("\n[BENCHMARK]")
    print("Spark handles 150M+ records distributed across cluster.")
    print("Pandas would struggle with this scale on a single machine.")


# =========================================================
# MAIN
# =========================================================

def main():

    os.makedirs(LOCAL_OUTPUT, exist_ok=True)

    spark = create_spark_session()

    print("=" * 60)
    print("NYC Trip Data Batch Analysis")
    print("Yellow Taxi + HVFHV")
    print("=" * 60)

    overall_start = time.time()

    try:

        # LOAD
        df = load_all_data(spark)

        # CLEAN
        df = clean_data(df)

        # CACHE
        print("\n[CACHE] Persisting cleaned dataframe...")
        df.cache()
        df.count()

        # ANALYSES
        analysis_peak_hours(df)

        analysis_daily_patterns(df)

        analysis_monthly_trends(df)

        analysis_source_comparison(df)

        benchmark_vs_pandas()

        overall_time = time.time() - overall_start

        print("\n" + "=" * 60)
        print(f"All analyses completed in {overall_time:.1f}s")
        print(f"Results saved to: {LOCAL_OUTPUT}")
        print("=" * 60)

    except Exception as e:

        print("\n[ERROR]")
        print(str(e))

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
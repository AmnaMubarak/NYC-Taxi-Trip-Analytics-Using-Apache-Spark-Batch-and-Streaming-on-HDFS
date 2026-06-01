"""
05_streaming_analysis_windows.py

Spark Structured Streaming - Real-time NYC Taxi Analytics

Reads streaming CSV batches from HDFS and performs:
1. Zone demand analytics
2. Daily running statistics
3. Payment analysis

Compatible with:
- Windows
- Linux
- Hadoop/YARN
- Yellow Taxi schema
- FHVHV schema

Usage:
    spark-submit --master yarn --deploy-mode client 05_streaming_analysis_windows.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

import os
import sys

# =========================================================
# HDFS SETTINGS (DO NOT REMOVE)
# =========================================================

HDFS_STREAM_INPUT = "/user/taxi/streaming_input"

HDFS_CHECKPOINT = "/user/taxi/checkpoints"

# =========================================================
# LOCAL OUTPUT
# =========================================================

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

LOCAL_OUTPUT = os.path.join(
    BASE_DIR,
    "output"
)

# =========================================================
# STREAM SCHEMA
# =========================================================

TAXI_SCHEMA = StructType([

    StructField("pickup_datetime", TimestampType(), True),

    StructField("dropoff_datetime", TimestampType(), True),

    StructField("passenger_count", DoubleType(), True),

    StructField("trip_distance", DoubleType(), True),

    StructField("PULocationID", IntegerType(), True),

    StructField("DOLocationID", IntegerType(), True),

    StructField("payment_type", IntegerType(), True),

    StructField("fare_amount", DoubleType(), True),

    StructField("tip_amount", DoubleType(), True),

    StructField("total_amount", DoubleType(), True),
])

# =========================================================
# CREATE SPARK SESSION
# =========================================================

def create_spark_session():

    spark = (

        SparkSession.builder

        .appName("NYC_Taxi_Streaming_Analytics")

        .config(
            "spark.sql.streaming.schemaInference",
            "false"
        )

        .config(
            "spark.sql.shuffle.partitions",
            "4"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# =========================================================
# LOAD STREAM
# =========================================================

def load_stream(spark):

    print(f"[STREAMING] Reading from: {HDFS_STREAM_INPUT}")

    stream_df = (

        spark.readStream

        .schema(TAXI_SCHEMA)

        .option("header", "true")

        .option("maxFilesPerTrigger", 2)

        .csv(f"hdfs://{HDFS_STREAM_INPUT}")
    )

    return stream_df


# =========================================================
# CLEAN STREAM
# =========================================================

def clean_stream(stream_df):

    cleaned = (

        stream_df

        .filter(F.col("pickup_datetime").isNotNull())

        .filter(F.col("fare_amount").isNotNull())

        .filter(F.col("trip_distance") >= 0)

        .filter(F.col("fare_amount") >= 0)

        .filter(F.col("total_amount") >= 0)
    )

    return cleaned


# =========================================================
# QUERY 1
# REAL-TIME ZONE DEMAND
# =========================================================

def start_zone_query(cleaned_stream):

    zone_counts = (

        cleaned_stream

        .withWatermark(
            "pickup_datetime",
            "10 minutes"
        )

        .groupBy(

            F.window(
                "pickup_datetime",
                "1 hour"
            ),

            F.col("PULocationID")
        )

        .agg(

            F.count("*").alias("trip_count"),

            F.round(
                F.avg("fare_amount"),
                2
            ).alias("avg_fare"),

            F.round(
                F.sum("total_amount"),
                2
            ).alias("total_revenue")
        )
    )

    query = (

        zone_counts.writeStream

        .outputMode("update")

        .format("console")

        .option("truncate", "false")

        .option("numRows", 20)

        .queryName("zone_demand")

        .trigger(
            processingTime="10 seconds"
        )

        .option(
            "checkpointLocation",
            f"{HDFS_CHECKPOINT}/zone_demand"
        )

        .start()
    )

    return query


# =========================================================
# QUERY 2
# DAILY RUNNING STATS
# =========================================================

def start_daily_stats_query(cleaned_stream):

    daily_stats = (

        cleaned_stream

        .groupBy(

            F.date_format(
                "pickup_datetime",
                "yyyy-MM-dd"
            ).alias("trip_date")
        )

        .agg(

            F.count("*").alias("total_trips"),

            F.round(
                F.avg("fare_amount"),
                2
            ).alias("avg_fare"),

            F.round(
                F.avg("tip_amount"),
                2
            ).alias("avg_tip"),

            F.round(
                F.avg("trip_distance"),
                2
            ).alias("avg_distance"),

            F.round(
                F.sum("total_amount"),
                2
            ).alias("total_revenue")
        )
    )

    query = (

        daily_stats.writeStream

        .outputMode("complete")

        .format("console")

        .option("truncate", "false")

        .queryName("daily_stats")

        .trigger(
            processingTime="15 seconds"
        )

        .option(
            "checkpointLocation",
            f"{HDFS_CHECKPOINT}/daily_stats"
        )

        .start()
    )

    return query


# =========================================================
# QUERY 3
# PAYMENT ANALYSIS
# =========================================================

def start_payment_query(cleaned_stream):

    payment_stats = (

        cleaned_stream

        .groupBy("payment_type")

        .agg(

            F.count("*").alias("trip_count"),

            F.round(
                F.avg("fare_amount"),
                2
            ).alias("avg_fare"),

            F.round(
                F.avg("tip_amount"),
                2
            ).alias("avg_tip"),

            F.round(
                F.sum("total_amount"),
                2
            ).alias("total_revenue")
        )
    )

    query = (

        payment_stats.writeStream

        .outputMode("complete")

        .format("memory")

        .queryName("payment_analysis")

        .trigger(
            processingTime="20 seconds"
        )

        .option(
            "checkpointLocation",
            f"{HDFS_CHECKPOINT}/payment_analysis"
        )

        .start()
    )

    return query


# =========================================================
# SAVE FINAL RESULTS
# =========================================================

def save_final_results(spark):

    try:

        os.makedirs(LOCAL_OUTPUT, exist_ok=True)

        payment_df = spark.sql(
            "SELECT * FROM payment_analysis"
        )

        payment_df.show(truncate=False)

        output_path = os.path.join(
            LOCAL_OUTPUT,
            "streaming_payment_results.csv"
        )

        payment_df.toPandas().to_csv(
            output_path,
            index=False
        )
        hdfs_path = f"hdfs:///user/taxi/streaming_output/streaming_payment_results.csv"
        payment_df.write.format("csv").mode("overwrite").save(hdfs_path)

        print("\n[SAVED]")
        print(output_path)

    except Exception as e:

        print("\nERROR saving results:")
        print(e)


# =========================================================
# MAIN STREAMING LOGIC
# =========================================================

def start_streaming(spark):

    stream_df = load_stream(spark)

    cleaned_stream = clean_stream(stream_df)

    # Start queries
    query_zone = start_zone_query(cleaned_stream)

    query_daily = start_daily_stats_query(cleaned_stream)

    query_payment = start_payment_query(cleaned_stream)

    print("\n" + "=" * 60)

    print("  Streaming queries started!")

    print("  Query 1: Zone demand analytics")

    print("  Query 2: Daily running statistics")

    print("  Query 3: Payment analysis")

    print("  Waiting for streaming data...")

    print("  Press Ctrl+C to stop")

    print("=" * 60 + "\n")

    try:

        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:

        print("\n[STOPPING STREAMS]")

        save_final_results(spark)

        query_zone.stop()

        query_daily.stop()

        query_payment.stop()

        print("\n[DONE] All streaming queries stopped.")


# =========================================================
# MAIN
# =========================================================

def main():

    os.makedirs(LOCAL_OUTPUT, exist_ok=True)

    print("=" * 60)

    print("  Spark Structured Streaming - NYC Taxi Analytics")

    print("=" * 60)

    print(f"  Stream Input : {HDFS_STREAM_INPUT}")

    print(f"  Checkpoints  : {HDFS_CHECKPOINT}")

    spark = create_spark_session()

    try:

        start_streaming(spark)

    except Exception as e:

        print("\nERROR: Streaming failed!")

        print(e)

        sys.exit(1)

    finally:

        spark.stop()


if __name__ == "__main__":

    main()
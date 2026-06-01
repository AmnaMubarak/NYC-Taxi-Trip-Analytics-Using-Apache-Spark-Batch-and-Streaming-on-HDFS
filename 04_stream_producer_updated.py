"""
04_stream_producer_windows.py

Stream Producer - Simulates real-time taxi trip data.
Reads parquet files and writes CSV micro-batches into HDFS
for Spark Structured Streaming.

Supports:
- Yellow Taxi dataset
- FHVHV dataset
- Windows/Linux/macOS

HDFS/Hadoop settings are preserved.

Usage:
    python 04_stream_producer_windows.py
"""

import pandas as pd
import os
import time
import subprocess
import sys
import platform

# =========================================================
# PATHS
# =========================================================

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

DATA_DIR = os.path.join(
    BASE_DIR,
    "data",
    "raw"
)

LOCAL_STREAM_DIR = os.path.join(
    BASE_DIR,
    "data",
    "stream_input"
)

# =========================================================
# HDFS SETTINGS (DO NOT REMOVE)
# =========================================================

HDFS_STREAM_DIR = "/user/taxi/streaming_input"

# =========================================================
# STREAM CONFIG
# =========================================================

BATCH_SIZE = 5000
INTERVAL_SECONDS = 5
MAX_BATCHES = 100


# =========================================================
# HDFS HELPER
# =========================================================

def run_hdfs_command(args, check=True, capture_output=False):
    """
    Execute HDFS command.
    Compatible with Windows/Linux/macOS.
    """

    return subprocess.run(
        ["hdfs"] + args,
        check=check,
        capture_output=capture_output,
        text=True,
        shell=(platform.system().lower() == "windows")
    )


# =========================================================
# SETUP DIRECTORIES
# =========================================================

def setup_directories():

    os.makedirs(LOCAL_STREAM_DIR, exist_ok=True)

    # Create HDFS directory
    run_hdfs_command([
        "dfs",
        "-mkdir",
        "-p",
        HDFS_STREAM_DIR
    ])

    # Remove old stream files
    run_hdfs_command(
        [
            "dfs",
            "-rm",
            "-f",
            f"{HDFS_STREAM_DIR}/*"
        ],
        check=False,
        capture_output=True
    )

    print(f"  Local staging: {LOCAL_STREAM_DIR}")
    print(f"  HDFS target:   {HDFS_STREAM_DIR}")


# =========================================================
# LOAD SOURCE DATA
# =========================================================

def load_source_data():
    """
    Load parquet source data.
    Supports:
    - Yellow Taxi schema
    - FHVHV schema
    """

    parquet_files = sorted([
        os.path.join(DATA_DIR, f)
        for f in os.listdir(DATA_DIR)
        if f.endswith(".parquet")
    ])

    if not parquet_files:
        print("ERROR: No parquet files found.")
        print("Run 01_download_data.py first!")
        sys.exit(1)

    print(f"  Loading source data from {len(parquet_files)} files...")

    frames = []

    # Use first 2 files for streaming simulation
    for f in parquet_files[:2]:

        print(f"    Reading: {os.path.basename(f)}")

        try:
            df = pd.read_parquet(f)

        except Exception as e:
            print(f"    ERROR reading {f}")
            print(e)
            continue

        # =================================================
        # YELLOW TAXI DATASET
        # =================================================

        if "tpep_pickup_datetime" in df.columns:

            required_columns = {
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "passenger_count": "passenger_count",
                "trip_distance": "trip_distance",
                "PULocationID": "PULocationID",
                "DOLocationID": "DOLocationID",
                "payment_type": "payment_type",
                "fare_amount": "fare_amount",
                "tip_amount": "tip_amount",
                "total_amount": "total_amount"
            }

            existing_cols = [
                col for col in required_columns.keys()
                if col in df.columns
            ]

            df = df[existing_cols]

            df = df.rename(columns={
                col: required_columns[col]
                for col in existing_cols
            })

        # =================================================
        # FHVHV DATASET
        # =================================================

        elif "pickup_datetime" in df.columns:

            required_columns = {
                "pickup_datetime": "pickup_datetime",
                "dropoff_datetime": "dropoff_datetime",
                "trip_miles": "trip_distance",
                "PULocationID": "PULocationID",
                "DOLocationID": "DOLocationID",
                "tips": "tip_amount",
                "base_passenger_fare": "fare_amount"
            }

            existing_cols = [
                col for col in required_columns.keys()
                if col in df.columns
            ]

            df = df[existing_cols]

            df = df.rename(columns={
                col: required_columns[col]
                for col in existing_cols
            })

            # Add missing columns
            if "passenger_count" not in df.columns:
                df["passenger_count"] = 1

            if "payment_type" not in df.columns:
                df["payment_type"] = 1

            if "tip_amount" not in df.columns:
                df["tip_amount"] = 0.0

            if "fare_amount" not in df.columns:
                df["fare_amount"] = 0.0

            # Generate total amount
            df["total_amount"] = (
                df["fare_amount"] + df["tip_amount"]
            )

        else:
            print(f"    Unsupported schema: {f}")
            continue

        frames.append(df)

    if not frames:
        print("ERROR: No compatible parquet files found!")
        sys.exit(1)

    # Combine all data
    data = pd.concat(frames, ignore_index=True)

    # Cleanup
    data = data.dropna(
        subset=[
            "pickup_datetime",
            "fare_amount"
        ]
    )

    # Shuffle rows for realistic streaming
    data = data.sample(
        frac=1,
        random_state=42
    ).reset_index(drop=True)

    print(f"  Loaded {len(data):,} records for streaming")

    return data


# =========================================================
# PRODUCE STREAM
# =========================================================

def produce_stream(data):

    print(f"\n  Streaming {BATCH_SIZE} rows every {INTERVAL_SECONDS} seconds...")
    print(f"  Total batches: {MAX_BATCHES}")
    print("  Press Ctrl+C to stop\n")

    total_records = 0
    start_idx = 0

    for batch_num in range(1, MAX_BATCHES + 1):

        end_idx = start_idx + BATCH_SIZE

        # Restart from beginning if dataset ends
        if end_idx > len(data):
            start_idx = 0
            end_idx = BATCH_SIZE

        batch = data.iloc[start_idx:end_idx].copy()

        start_idx = end_idx

        timestamp = int(time.time() * 1000)

        filename = f"batch_{batch_num:04d}_{timestamp}.csv"

        local_path = os.path.join(
            LOCAL_STREAM_DIR,
            filename
        )

        # Save CSV locally
        batch.to_csv(
            local_path,
            index=False
        )

        # Upload to HDFS
        run_hdfs_command(
            [
                "dfs",
                "-put",
                "-f",
                local_path,
                f"{HDFS_STREAM_DIR}/{filename}"
            ],
            check=True,
            capture_output=True
        )

        # Remove local temp file
        if os.path.exists(local_path):
            os.remove(local_path)

        total_records += len(batch)

        print(
            f"  [Batch {batch_num:03d}/{MAX_BATCHES}] "
            f"Sent {len(batch):,} rows | "
            f"Total: {total_records:,} | "
            f"{filename}"
        )

        # Wait before next batch
        if batch_num < MAX_BATCHES:
            time.sleep(INTERVAL_SECONDS)

    print(
        f"\n  Stream complete! "
        f"Sent {total_records:,} rows "
        f"in {MAX_BATCHES} batches."
    )


# =========================================================
# MAIN
# =========================================================

def main():

    print("=" * 60)
    print("  Stream Producer - Simulating Real-Time Taxi Data")
    print("=" * 60)

    print(f"  Operating System: {platform.system()}")

    setup_directories()

    data = load_source_data()

    try:

        produce_stream(data)

    except KeyboardInterrupt:

        print("\n  Stream stopped by user.")

    except subprocess.CalledProcessError as e:

        print("\nERROR: HDFS command failed!")

        if e.stderr:
            print(e.stderr)

        sys.exit(1)

    except FileNotFoundError:

        print("\nERROR: `hdfs` command not found.")
        print("Ensure Hadoop is installed and added to PATH.")

        sys.exit(1)

    except Exception as e:

        print(f"\nERROR: {e}")

        sys.exit(1)


if __name__ == "__main__":
    main()
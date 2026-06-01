"""
Download NYC Taxi Trip Data from TLC website.

Downloads TWO types:
  1. Yellow Taxi - ~45 MB/month parquet (~3M records each)
  2. HVFHV (Uber/Lyft) - ~400-700 MB/month parquet (~20M records each)

Total: ~6-8 GB of real trip data = ~150+ million records
"""

import os
import requests
import sys

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")

# Yellow taxi - smaller files, 6 months
YELLOW_MONTHS = [
    "2023-01", "2023-02", "2023-03",
    "2023-04", "2023-05", "2023-06",
]

# HVFHV (Uber/Lyft) - BIG files (~400-700 MB each), 6 months
HVFHV_MONTHS = [
    "2023-01", "2023-02", "2023-03",
    "2023-04", "2023-05", "2023-06",
]


def download_file(url, filepath):
    if os.path.exists(filepath):
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        print(f"  [SKIP] Already exists ({size_mb:.1f} MB)")
        return filepath

    print(f"  Downloading from {url}")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0

    with open(filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            if total_size > 0:
                pct = (downloaded / total_size) * 100
                mb = downloaded / (1024 * 1024)
                total_mb = total_size / (1024 * 1024)
                print(f"\r  Progress: {pct:.1f}% ({mb:.0f}/{total_mb:.0f} MB)", end="", flush=True)

    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"\n  [DONE] {size_mb:.1f} MB")
    return filepath


def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    print("=" * 60)
    print("  NYC Trip Data Downloader")
    print("=" * 60)

    total_size = 0
    total_files = 0

    # Download Yellow Taxi data
    print(f"\n--- Yellow Taxi Data ({len(YELLOW_MONTHS)} months) ---")
    for month in YELLOW_MONTHS:
        filename = f"yellow_tripdata_{month}.parquet"
        filepath = os.path.join(DOWNLOAD_DIR, filename)
        url = f"{BASE_URL}/{filename}"
        print(f"\n[{month}] {filename}")
        try:
            download_file(url, filepath)
            total_size += os.path.getsize(filepath)
            total_files += 1
        except Exception as e:
            print(f"  [ERROR] {e}")

    # Download HVFHV (Uber/Lyft) data - THE BIG FILES
    print(f"\n--- HVFHV / Uber+Lyft Data ({len(HVFHV_MONTHS)} months) ---")
    for month in HVFHV_MONTHS:
        filename = f"fhvhv_tripdata_{month}.parquet"
        filepath = os.path.join(DOWNLOAD_DIR, filename)
        url = f"{BASE_URL}/{filename}"
        print(f"\n[{month}] {filename}")
        try:
            download_file(url, filepath)
            total_size += os.path.getsize(filepath)
            total_files += 1
        except Exception as e:
            print(f"  [ERROR] {e}")

    total_gb = total_size / (1024 * 1024 * 1024)
    print("\n" + "=" * 60)
    print(f"  Download complete!")
    print(f"  Files: {total_files}")
    print(f"  Total size: {total_gb:.2f} GB")
    print(f"  Location: {DOWNLOAD_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
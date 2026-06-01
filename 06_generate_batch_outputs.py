import pandas as pd
import os

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BASE, "data", "raw")
OUT = os.path.join(BASE, "output")

os.makedirs(OUT, exist_ok=True)

files = [f for f in os.listdir(DATA) if f.endswith(".parquet")]

df = pd.concat(
    [pd.read_parquet(os.path.join(DATA, f)) for f in files[:2]],
    ignore_index=True
)

# ---------------------------
# Popular zones
# ---------------------------
if "PULocationID" in df.columns:
    zone = df.groupby("PULocationID").size().reset_index(name="total_pickups")
    zone["avg_fare"] = df.groupby("PULocationID")["fare_amount"].mean().values if "fare_amount" in df.columns else 0
    zone.to_csv(os.path.join(OUT, "popular_pickup_zones.csv"), index=False)

# ---------------------------
# Fare distribution
# ---------------------------
if "avg_fare" in df.columns:
    bins = [0,10,20,30,50,100,999999]
    labels = ["Under $10","$10-$20","$20-$30","$30-$50","$50-$100","$100+"]

    df["fare_bucket"] = pd.cut(df["avg_fare"], bins=bins, labels=labels)

    fare = df.groupby("fare_bucket").size().reset_index(name="count")
    fare.to_csv(os.path.join(OUT, "fare_distribution.csv"), index=False)

# ---------------------------
# Payment types
# ---------------------------
if "payment_type" in df.columns:
    pay = df.groupby("payment_type").agg(
        total_trips=("payment_type","count"),
        avg_tip=("tip_amount","mean") if "tip_amount" in df.columns else ("payment_type","count")
    ).reset_index()

    pay.to_csv(os.path.join(OUT, "payment_types.csv"), index=False)

print("Batch CSV files generated in output/")
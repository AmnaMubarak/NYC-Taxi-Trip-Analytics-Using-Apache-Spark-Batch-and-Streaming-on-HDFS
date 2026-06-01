"""
06_visualize_results_windows.py

Visualization of batch and streaming analysis results.

Reads CSV files from output/ directory and generates charts.

Compatible with:
- Windows
- Linux
- macOS

Usage:
    python 06_visualize_results_windows.py
"""

import os
import pandas as pd

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import seaborn as sns

# =========================================================
# PATHS
# =========================================================

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

OUTPUT_DIR = os.path.join(
    BASE_DIR,
    "output"
)

CHART_DIR = os.path.join(
    OUTPUT_DIR,
    "charts"
)

# =========================================================
# SETUP
# =========================================================

def setup():

    os.makedirs(CHART_DIR, exist_ok=True)

    sns.set_theme(style="whitegrid")

    plt.rcParams["figure.figsize"] = (12, 6)

    plt.rcParams["figure.dpi"] = 150

    plt.rcParams["savefig.bbox"] = "tight"


# =========================================================
# SAFE CSV READER
# =========================================================

def read_csv_safe(filename):

    filepath = os.path.join(
        OUTPUT_DIR,
        filename
    )

    if not os.path.exists(filepath):

        print(f"  [SKIP] {filename} not found")

        return None

    try:

        df = pd.read_csv(filepath)

        if df.empty:

            print(f"  [SKIP] {filename} is empty")

            return None

        print(f"  [LOADED] {filename}")

        return df

    except Exception as e:

        print(f"  [ERROR] Could not read {filename}")

        print(e)

        return None


# =========================================================
# PEAK HOURS
# =========================================================

def plot_peak_hours():

    df = read_csv_safe("peak_hours.csv")

    if df is None:
        return

    required_cols = [
        "hour",
        "total_trips",
        "avg_fare"
    ]

    if not all(col in df.columns for col in required_cols):

        print("  [SKIP] peak_hours.csv missing columns")

        return

    fig, ax1 = plt.subplots(figsize=(14, 6))

    ax1.bar(
        df["hour"],
        df["total_trips"],
        color="#2196F3",
        alpha=0.75
    )

    ax1.set_xlabel("Hour of Day")

    ax1.set_ylabel("Total Trips")

    ax1.set_xticks(range(24))

    ax2 = ax1.twinx()

    ax2.plot(
        df["hour"],
        df["avg_fare"],
        color="#FF5722",
        linewidth=2,
        marker="o"
    )

    ax2.set_ylabel("Average Fare ($)")

    plt.title(
        "NYC Taxi - Trip Volume and Average Fare by Hour",
        fontsize=14,
        fontweight="bold"
    )

    plt.savefig(
        os.path.join(CHART_DIR, "peak_hours.png")
    )

    plt.close()

    print("  [SAVED] peak_hours.png")


# =========================================================
# DAILY PATTERNS
# =========================================================

def plot_daily_patterns():

    df = read_csv_safe("daily_patterns.csv")

    if df is None:
        return

    required_cols = [
        "day_name",
        "total_trips",
        "avg_fare",
        "avg_tip"
    ]

    if not all(col in df.columns for col in required_cols):

        print("  [SKIP] daily_patterns.csv missing columns")

        return

    if "day_of_week" in df.columns:

        df = df.sort_values("day_of_week")

    fig, axes = plt.subplots(
        1,
        3,
        figsize=(18, 5)
    )

    # Trips
    axes[0].bar(
        df["day_name"],
        df["total_trips"],
        color="#4CAF50"
    )

    axes[0].set_title("Trips by Day")

    axes[0].tick_params(
        axis="x",
        rotation=45
    )

    # Fare
    axes[1].bar(
        df["day_name"],
        df["avg_fare"],
        color="#FF9800"
    )

    axes[1].set_title("Average Fare")

    axes[1].tick_params(
        axis="x",
        rotation=45
    )

    # Tips
    axes[2].bar(
        df["day_name"],
        df["avg_tip"],
        color="#9C27B0"
    )

    axes[2].set_title("Average Tip")

    axes[2].tick_params(
        axis="x",
        rotation=45
    )

    plt.suptitle(
        "NYC Taxi - Daily Patterns",
        fontsize=14,
        fontweight="bold"
    )

    plt.savefig(
        os.path.join(CHART_DIR, "daily_patterns.png")
    )

    plt.close()

    print("  [SAVED] daily_patterns.png")


# =========================================================
# POPULAR ZONES
# =========================================================

def plot_popular_zones():

    df = read_csv_safe("popular_pickup_zones.csv")

    if df is None:
        return

    required_cols = [
        "PULocationID",
        "total_pickups"
    ]

    if not all(col in df.columns for col in required_cols):

        print("  [SKIP] popular_pickup_zones.csv missing columns")

        return

    df = df.sort_values(
        "total_pickups",
        ascending=True
    )

    fig, ax = plt.subplots(figsize=(12, 7))

    ax.barh(
        df["PULocationID"].astype(str),
        df["total_pickups"],
        color="#2196F3"
    )

    ax.set_xlabel("Total Pickups")

    ax.set_ylabel("Zone ID")

    ax.set_title(
        "Top Pickup Zones",
        fontsize=14,
        fontweight="bold"
    )

    plt.savefig(
        os.path.join(CHART_DIR, "popular_zones.png")
    )

    plt.close()

    print("  [SAVED] popular_zones.png")


# =========================================================
# FARE DISTRIBUTION
# =========================================================

def plot_fare_distribution():

    df = read_csv_safe("fare_distribution.csv")

    if df is None:
        return

    required_cols = [
        "fare_bucket",
        "count"
    ]

    if not all(col in df.columns for col in required_cols):

        print("  [SKIP] fare_distribution.csv missing columns")

        return

    order = [
        "Under $10",
        "$10-$20",
        "$20-$30",
        "$30-$50",
        "$50-$100",
        "$100+"
    ]

    df["fare_bucket"] = pd.Categorical(
        df["fare_bucket"],
        categories=order,
        ordered=True
    )

    df = df.sort_values("fare_bucket")

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.bar(
        df["fare_bucket"],
        df["count"],
        color=[
            "#4CAF50",
            "#8BC34A",
            "#FFC107",
            "#FF9800",
            "#FF5722",
            "#F44336"
        ][:len(df)]
    )

    ax.set_xlabel("Fare Range")

    ax.set_ylabel("Trips")

    ax.set_title(
        "NYC Taxi - Fare Distribution",
        fontsize=14,
        fontweight="bold"
    )

    plt.savefig(
        os.path.join(CHART_DIR, "fare_distribution.png")
    )

    plt.close()

    print("  [SAVED] fare_distribution.png")


# =========================================================
# PAYMENT TYPES
# =========================================================

def plot_payment_types():

    df = read_csv_safe("payment_types.csv")

    if df is None:
        return

    required_cols = [
        "payment_type",
        "total_trips",
        "avg_tip"
    ]

    if not all(col in df.columns for col in required_cols):

        print("  [SKIP] payment_types.csv missing columns")

        return

    labels_map = {

        1: "Credit Card",

        2: "Cash",

        3: "No Charge",

        4: "Dispute",

        5: "Unknown"
    }

    df["payment_label"] = (
        df["payment_type"]
        .map(labels_map)
        .fillna("Other")
    )

    fig, axes = plt.subplots(
        1,
        2,
        figsize=(14, 6)
    )

    # Pie chart
    axes[0].pie(
        df["total_trips"],
        labels=df["payment_label"],
        autopct="%1.1f%%",
        startangle=90
    )

    axes[0].set_title(
        "Payment Distribution"
    )

    # Tips chart
    axes[1].bar(
        df["payment_label"],
        df["avg_tip"],
        color="#9C27B0"
    )

    axes[1].set_title(
        "Average Tip by Payment Method"
    )

    axes[1].tick_params(
        axis="x",
        rotation=30
    )

    plt.suptitle(
        "NYC Taxi - Payment Analysis",
        fontsize=14,
        fontweight="bold"
    )

    plt.savefig(
        os.path.join(CHART_DIR, "payment_types.png")
    )

    plt.close()

    print("  [SAVED] payment_types.png")


# =========================================================
# MONTHLY TRENDS
# =========================================================

def plot_monthly_trends():

    df = read_csv_safe("monthly_trends.csv")

    if df is None:
        return

    if "month" not in df.columns:

        print("  [SKIP] month column missing")

        return

    if "total_trips" not in df.columns:

        print("  [SKIP] total_trips column missing")

        return

    # =====================================================
    # FIND REVENUE COLUMN
    # =====================================================

    revenue_col = None

    revenue_candidates = [

        "total_fare_revenue",

        "fare_revenue",

        "total_revenue",

        "revenue",

        "fare_amount"
    ]

    for col in revenue_candidates:

        if col in df.columns:

            revenue_col = col

            break

    # =====================================================
    # FIND TIP COLUMN
    # =====================================================

    tips_col = None

    tip_candidates = [

        "total_tips",

        "tips",

        "tip_amount"
    ]

    for col in tip_candidates:

        if col in df.columns:

            tips_col = col

            break

    fig, axes = plt.subplots(
        2,
        1,
        figsize=(14, 10)
    )

    # =====================================================
    # TOP CHART
    # =====================================================

    axes[0].plot(
        df["month"],
        df["total_trips"],
        marker="o",
        linewidth=2,
        color="#2196F3"
    )

    axes[0].fill_between(
        df["month"],
        df["total_trips"],
        alpha=0.15,
        color="#2196F3"
    )

    axes[0].set_title(
        "Monthly Trip Volume",
        fontweight="bold"
    )

    axes[0].set_ylabel("Trips")

    axes[0].tick_params(
        axis="x",
        rotation=45
    )

    # =====================================================
    # BOTTOM CHART
    # =====================================================

    if revenue_col:

        axes[1].plot(
            df["month"],
            df[revenue_col],
            marker="s",
            linewidth=2,
            color="#4CAF50",
            label="Revenue"
        )

    if tips_col:

        axes[1].plot(
            df["month"],
            df[tips_col],
            marker="^",
            linewidth=2,
            color="#FF9800",
            label="Tips"
        )

    axes[1].set_title(
        "Monthly Revenue Trends",
        fontweight="bold"
    )

    axes[1].set_ylabel("Amount ($)")

    axes[1].tick_params(
        axis="x",
        rotation=45
    )

    axes[1].legend()

    plt.suptitle(
        "NYC Taxi - Monthly Trends",
        fontsize=14,
        fontweight="bold"
    )

    plt.savefig(
        os.path.join(CHART_DIR, "monthly_trends.png")
    )

    plt.close()

    print("  [SAVED] monthly_trends.png")


# =========================================================
# MAIN
# =========================================================

def main():

    setup()

    print("=" * 60)

    print("  Generating Visualizations")

    print("=" * 60)

    plot_peak_hours()

    plot_daily_patterns()

    plot_popular_zones()

    plot_fare_distribution()

    plot_payment_types()

    plot_monthly_trends()

    chart_files = [

        f for f in os.listdir(CHART_DIR)

        if f.endswith(".png")
    ]

    print(f"\n  Generated {len(chart_files)} charts")

    print(f"  Output Folder: {CHART_DIR}")

    print("=" * 60)


if __name__ == "__main__":

    main()
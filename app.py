import io
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import requests
import seaborn as sns
from boto3.dynamodb.conditions import Key

# Use 'Agg' for non-interactive environments (like Docker/Kubernetes)
matplotlib.use("Agg")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Constants
TIDE_API     = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
STATION_IDS  = ["8447435", "8594900", "8725520", "8771450"]
TABLE_NAME   = os.environ["DYNAMODB_TABLE"]
S3_BUCKET    = os.environ["S3_BUCKET"]
AWS_REGION   = os.environ.get("AWS_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# Step 1 — Fetch current tide for a single station
# ---------------------------------------------------------------------------
def fetch_single_tide(station_id: str) -> dict:
    """Fetch the latest 6-minute water level for a specific station."""
    params = {
        "station": station_id,
        "product": "water_level",
        "date": "latest",
        "datum": "MLLW",
        "units": "english",
        "time_zone": "gmt",
        "format": "json"
    }
    resp = requests.get(TIDE_API, params=params, timeout=10)
    resp.raise_for_status()
    d = resp.json()
    
    # Extract the value from the first data point
    return {
        "station_id": station_id,
        "station_name": d['metadata']['name'],
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "water_level": Decimal(str(d['data'][0]['v']))
    }

# ---------------------------------------------------------------------------
# Step 2 — Query DynamoDB for previous data
# ---------------------------------------------------------------------------
def get_previous_two(table, station_id: str) -> list:
    """Return the 2 latest stored items for a station."""
    resp = table.query(
        KeyConditionExpression=Key("station_id").eq(station_id),
        ScanIndexForward=False,
        Limit=2,
    )
    return resp.get("Items", [])

# ---------------------------------------------------------------------------
# Step 3 — Trend Analysis
# ---------------------------------------------------------------------------
def tide_trend_analysis(current_height: Decimal, previous_items: list) -> tuple[str, Decimal, Decimal]:
    if len(previous_items) < 2:
        return "COLLECTING_DATA", Decimal("0"), Decimal("0")

    p1_height = Decimal(str(previous_items[0]["water_level"]))
    p2_height = Decimal(str(previous_items[1]["water_level"]))

    delta_now = current_height - p1_height
    delta_last = p1_height - p2_height

    if delta_now > 0 and delta_last <= 0:
        trend = "TIDE SWITCH (RISING/LOW)"
    elif delta_now < 0 and delta_last >= 0:
        trend = "TIDE SWITCH (FALLING/HIGH)"
    elif delta_now > 0:
        trend = "STILL RISING"
    elif delta_now < 0:
        trend = "STILL FALLING"
    else:
        trend = "STABLE"

    return trend, delta_now, delta_last

# ---------------------------------------------------------------------------
# Step 4 — Fetch History
# ---------------------------------------------------------------------------
def fetch_history(table) -> pd.DataFrame:
    all_items = []
    for station_id in STATION_IDS:
        kwargs = dict(
            KeyConditionExpression=Key("station_id").eq(station_id),
            ScanIndexForward=True,
        )
        while True:
            resp = table.query(**kwargs)
            all_items.extend(resp.get("Items", []))
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    if not all_items:
        return pd.DataFrame()

    df = pd.DataFrame(all_items)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["water_level"] = df["water_level"].astype(float)
    return df.sort_values("timestamp").reset_index(drop=True)

# ---------------------------------------------------------------------------
# Step 5 — Generate Plot
# ---------------------------------------------------------------------------
def generate_plot(df: pd.DataFrame) -> io.BytesIO | None:
    if df.empty or len(df) < 2:
        return None

    sns.set_theme(style="whitegrid", context="talk")
    fig, ax = plt.subplots(figsize=(14, 7))

    sns.lineplot(data=df, x="timestamp", y="water_level", hue="station_name", 
                 ax=ax, linewidth=2, marker='o', markersize=4)

    switches = df[df["trend"].str.contains("SWITCH", na=False)]
    if not switches.empty:
        ax.scatter(switches["timestamp"], switches["water_level"], color="black", s=100, zorder=5)
        for _, row in switches.iterrows():
            icon = "🌊" if "RISING" in row["trend"] else "⚓"
            ax.annotate(icon, xy=(row["timestamp"], row["water_level"]), 
                        xytext=(0, 10), textcoords="offset points", ha="center")

    ax.set_title(f"Coastal Water Levels (Updated: {datetime.now().strftime('%H:%M UTC')})")
    ax.set_ylabel("Height Above MLLW (ft)")
    ax.legend(title="Station Location", bbox_to_anchor=(1.05, 1), loc='upper left')

    plt.xticks(rotation=25)
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    return buf

# ---------------------------------------------------------------------------
# Step 6 — Push to S3
# ---------------------------------------------------------------------------
def push_plot(buf: io.BytesIO) -> None:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="tide-heights.png",
        Body=buf.getvalue(),
        ContentType="image/png",
    )
    log.info(f"Uploaded tide-heights.png to s3://{S3_BUCKET}")

# ---------------------------------------------------------------------------
# Step 7 — Generate and Push Parquet Data
# ---------------------------------------------------------------------------
def push_parquet_data(df: pd.DataFrame) -> None:
    """Converts the dataframe to Parquet and uploads it to S3."""
    if df.empty:
        log.warning("History DataFrame is empty. Skipping Parquet export.")
        return

    try:
        # 1. Convert DataFrame to Parquet in memory
        parquet_buf = io.BytesIO()
        # We ensure index=False to keep the file clean
        df.to_parquet(parquet_buf, engine='pyarrow', index=False)
        parquet_buf.seek(0)

        # 2. Upload to S3
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key="data.parquet",
            Body=parquet_buf.getvalue(),
            ContentType="application/octet-stream", # Standard for binary data files
        )
        log.info(f"✅ Successfully uploaded data.parquet to s3://{S3_BUCKET}")
        
        # Construct the URL for your logs
        public_url = f"http://{S3_BUCKET}.s3-website-{AWS_REGION}.amazonaws.com/data.parquet"
        log.info(f"Parquet file available at: {public_url}")

    except Exception as e:
        log.error(f"Failed to generate or upload Parquet: {e}")

# ---------------------------------------------------------------------------
# Updated Main Execution
# ---------------------------------------------------------------------------
def main():
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.Table(TABLE_NAME)

    for station_id in STATION_IDS:
        try:
            previous_items = get_previous_two(table, station_id)
            current_entry = fetch_single_tide(station_id) 

            trend, delta_now, _ = tide_trend_analysis(
                current_entry["water_level"], 
                previous_items
            )

            current_entry["trend"] = trend
            current_entry["delta_ft"] = delta_now

            table.put_item(Item=current_entry)
            log.info(f"TIDE | {current_entry['station_name']} | lvl={current_entry['water_level']:.2f}ft | {trend}")

        except Exception as e:
            log.error(f"Error processing {station_id}: {e}")

    # Fetch history once for both the plot and the parquet file
    history_df = fetch_history(table)
    
    # Existing Step 5 & 6
    plot_buf = generate_plot(history_df)
    if plot_buf:
        push_plot(plot_buf)

    # NEW Step 7
    push_parquet_data(history_df)

if __name__ == "__main__":
    main()
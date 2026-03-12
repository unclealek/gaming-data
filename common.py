"""
common.py
---------
Shared config, constants, and utility functions for all generators.
Databricks version — output is written as Parquet to DBFS via Spark.
"""

import random
import hashlib
from datetime import datetime, timedelta

from faker import Faker
from pyspark.sql import SparkSession

fake = Faker()
random.seed()

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
BASE_OUTPUT_DIR = "dbfs:/data/raw"
NUM_PLAYERS = 5000
START_DATE = datetime(2026, 1, 1)
END_DATE = datetime(2026, 3, 31)

COUNTRIES = ["FI", "SE", "NO", "DK", "DE", "GB", "US", "CA", "NG", "IN"]
DEVICES = ["ios", "android"]
ACQUISITION_CHANNELS = ["organic", "facebook_ads", "google_ads", "tiktok_ads", "referral"]

ITEM_CATALOG = [
    {"item_name": "gold_pack_small",  "item_type": "currency",     "price_usd": 1.99},
    {"item_name": "gold_pack_medium", "item_type": "currency",     "price_usd": 4.99},
    {"item_name": "gold_pack_large",  "item_type": "currency",     "price_usd": 9.99},
    {"item_name": "starter_bundle",   "item_type": "bundle",       "price_usd": 3.99},
    {"item_name": "battle_pass",      "item_type": "subscription", "price_usd": 7.99},
    {"item_name": "energy_refill",    "item_type": "consumable",   "price_usd": 0.99},
    {"item_name": "epic_skin",        "item_type": "cosmetic",     "price_usd": 12.99},
]

LEVELS = list(range(1, 21))

# ---------------------------------------------------
# STABLE ATTRIBUTE HELPERS
# Deterministic per user_id so country/device/etc.
# never change between runs for the same player.
# ---------------------------------------------------
def stable_choice(user_id: str, options: list, salt: str = ""):
    """Always returns the same option for a given user_id + salt combo."""
    key = f"{user_id}{salt}".encode()
    hash_int = int(hashlib.md5(key).hexdigest(), 16)
    return options[hash_int % len(options)]


def stable_weighted_choice(user_id: str, options_with_weights: list, salt: str = ""):
    """Deterministic weighted choice for a given user_id + salt."""
    key = f"{user_id}{salt}".encode()
    hash_int = int(hashlib.md5(key).hexdigest(), 16)
    stable_float = (hash_int % 1_000_000) / 1_000_000

    cumulative = 0.0
    total = sum(w for _, w in options_with_weights)
    for value, weight in options_with_weights:
        cumulative += weight / total
        if stable_float < cumulative:
            return value
    return options_with_weights[-1][0]


# ---------------------------------------------------
# GENERAL HELPERS
# ---------------------------------------------------
def weighted_choice(options_with_weights: list):
    """Random weighted choice — varies each run."""
    values  = [x[0] for x in options_with_weights]
    weights = [x[1] for x in options_with_weights]
    return random.choices(values, weights=weights, k=1)[0]


def random_datetime(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def isoformat_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------
# CORRUPTION HELPERS
# ---------------------------------------------------
def maybe_corrupt_timestamp(ts: str) -> str:
    roll = random.random()
    if roll < 0.01:
        return ts.replace("T", " ")
    if roll < 0.015:
        return "INVALID_TIMESTAMP"
    return ts


def maybe_corrupt_device(device: str) -> str:
    roll = random.random()
    if roll < 0.03:
        return device.upper()
    if roll < 0.05:
        return device.capitalize()
    if roll < 0.055:
        return "andr0id"
    return device


def maybe_corrupt_country(country: str) -> str:
    roll = random.random()
    if roll < 0.02:
        return country.lower()
    if roll < 0.025:
        return "UNK"
    return country



# ---------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------
def get_spark() -> SparkSession:
    """Get or create a SparkSession. Works in notebooks and jobs."""
    return SparkSession.builder.getOrCreate()


# ---------------------------------------------------
# SPARK WRITER
# ---------------------------------------------------
def write_parquet(records: list, path: str, partition_by: str = None):
    """
    Convert a list of dicts to a Spark DataFrame and write as Parquet.

    Args:
        records:      list of dicts (one per row)
        path:         full dbfs:/ output path
        partition_by: column name to partition by (e.g. "event_date")
    """
    spark  = get_spark()
    df     = spark.createDataFrame(records)
    writer = df.write.mode("overwrite").format("parquet")

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.save(path)
    print(f"  Written {len(records):,} rows → {path}")


def add_date_partition_col(records: list, timestamp_field: str) -> list:
    """
    Add an 'event_date' string column (YYYY-MM-DD) derived from a timestamp
    field so Spark can partition by it cleanly.
    Records with invalid/missing timestamps get event_date = 'unknown'.
    """
    for rec in records:
        ts = rec.get(timestamp_field)
        if not ts or ts == "INVALID_TIMESTAMP":
            rec["event_date"] = "unknown"
        else:
            try:
                normalized        = ts.replace(" ", "T").replace("Z", "")
                dt                = datetime.strptime(normalized, "%Y-%m-%dT%H:%M:%S")
                rec["event_date"] = dt.strftime("%Y-%m-%d")
            except Exception:
                rec["event_date"] = "unknown"
    return records

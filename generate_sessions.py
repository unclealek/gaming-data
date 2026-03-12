"""
generate_sessions.py
--------------------
Generates play sessions for each player and writes them to:
    data/raw/sessions/<year>/<month>/<day>/sessions_001.json

Depends on:
    generate_players.py  (reads profiles_001.json if run standalone)

Each session record carries the player's stable country + device
as the base value, with random corruption applied on top.
"""

import uuid
import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from common import (
    BASE_OUTPUT_DIR,
    DEVICES,
    START_DATE,
    END_DATE,
    weighted_choice,
    random_datetime,
    isoformat_z,
    maybe_corrupt_device,
    maybe_corrupt_country,
    get_spark,
    write_parquet,
    add_date_partition_col,
)


def generate_sessions(players: list) -> tuple[list, dict]:
    sessions = []
    sessions_by_user = {}

    for p in players:
        user_id = p["user_id"]

        # skip duplicate profile rows — only process each user once
        if user_id in sessions_by_user:
            continue

        install_date_str = p.get("install_date")
        if not install_date_str:
            sessions_by_user[user_id] = []
            continue

        install_dt = datetime.strptime(install_date_str, "%Y-%m-%d")
        days_since_install = (END_DATE - install_dt).days
        if days_since_install < 0:
            sessions_by_user[user_id] = []
            continue

        engagement = p["engagement_segment"]
        if engagement == "casual":
            mean_active_days = min(days_since_install, random.randint(2, 10))
        elif engagement == "regular":
            mean_active_days = min(days_since_install, random.randint(8, 25))
        else:
            mean_active_days = min(days_since_install, random.randint(20, 50))

        active_days = sorted(set(
            install_dt + timedelta(days=random.randint(0, days_since_install))
            for _ in range(mean_active_days)
        ))

        user_sessions = []

        for active_day in active_days:
            num_sessions_today = weighted_choice([
                (1, 0.65), (2, 0.25), (3, 0.08), (4, 0.02)
            ])

            for _ in range(num_sessions_today):
                session_start = active_day.replace(
                    hour=random.randint(7, 23),
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59),
                )
                duration_minutes = weighted_choice([
                    (random.randint(3, 10),  0.30),
                    (random.randint(10, 25), 0.45),
                    (random.randint(25, 45), 0.20),
                    (random.randint(45, 90), 0.05),
                ])
                session_end = session_start + timedelta(minutes=duration_minutes)

                # Use the player's stable country/device as the base
                base_device  = p["device"]  if p.get("device")  else random.choice(DEVICES)
                base_country = p["country"] if p.get("country") else random.choice(["FI", "SE", "NO"])

                record = {
                    "session_id":               str(uuid.uuid4()),
                    "user_id":                  user_id,
                    "session_start":            isoformat_z(session_start),
                    "session_end":              isoformat_z(session_end),
                    "session_duration_seconds": duration_minutes * 60,
                    "device":                   maybe_corrupt_device(base_device),
                    "country":                  maybe_corrupt_country(base_country),
                    "app_version":              p["app_version"],
                }

                # intentional bad values
                if random.random() < 0.005:
                    record["session_duration_seconds"] = -1
                if random.random() < 0.003:
                    record["session_end"] = None

                sessions.append(record)
                user_sessions.append(record)

        sessions_by_user[user_id] = user_sessions

    # ~0.5% duplicate rows
    if sessions:
        sessions.extend(random.sample(sessions, k=max(1, len(sessions) // 200)))

    return sessions, sessions_by_user


def load_players() -> list:
    """Load player profiles from DBFS when running standalone."""
    spark = get_spark()
    df    = spark.read.parquet(f"{BASE_OUTPUT_DIR}/player_profiles")
    return [row.asDict() for row in df.collect()]


def main():
    print("Loading player profiles...")
    players = load_players()

    print("Generating sessions...")
    sessions, _ = generate_sessions(players)
    sessions     = add_date_partition_col(sessions, "session_start")

    write_parquet(
        sessions,
        f"{BASE_OUTPUT_DIR}/sessions",
        partition_by="event_date",
    )

    print(f"Done. Sessions written: {len(sessions):,}")
    return sessions


if __name__ == "__main__":
    main()

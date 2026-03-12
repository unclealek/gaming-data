"""
generate_purchases.py
---------------------
Generates in-app purchase records and writes them to:
    data/raw/purchases/<year>/<month>/<day>/purchases_001.json

Purchase frequency is driven by each player's stable payer_segment:
    non_payer    →  1% chance per session
    light_payer  →  6% chance per session
    whale        → 18% chance per session

Depends on:
    generate_players.py   (reads profiles_001.json if run standalone)
    generate_sessions.py  (reads sessions partitions if run standalone)
"""

import uuid
import random
from datetime import datetime, timedelta

from common import (
    BASE_OUTPUT_DIR,
    ITEM_CATALOG,
    START_DATE,
    END_DATE,
    weighted_choice,
    random_datetime,
    isoformat_z,
    maybe_corrupt_timestamp,
    get_spark,
    write_parquet,
    add_date_partition_col,
)


def generate_purchases(players: list, sessions_by_user: dict) -> list:
    purchases = []
    seen_users = set()

    for p in players:
        user_id = p["user_id"]
        if user_id in seen_users:
            continue
        seen_users.add(user_id)

        payer_segment = p["payer_segment"]
        user_sessions = sessions_by_user.get(user_id, [])
        if not user_sessions:
            continue

        if payer_segment == "non_payer":
            purchase_prob = 0.01
        elif payer_segment == "light_payer":
            purchase_prob = 0.06
        else:
            purchase_prob = 0.18

        for s in user_sessions:
            if random.random() >= purchase_prob:
                continue

            num_purchases = weighted_choice([(1, 0.85), (2, 0.13), (3, 0.02)])

            try:
                session_start = datetime.strptime(
                    s["session_start"].replace(" ", "T").replace("Z", ""),
                    "%Y-%m-%dT%H:%M:%S"
                )
            except Exception:
                session_start = random_datetime(START_DATE, END_DATE)

            for _ in range(num_purchases):
                item = random.choice(ITEM_CATALOG)
                purchase_time = session_start + timedelta(
                    minutes=random.randint(1, 30),
                    seconds=random.randint(0, 59),
                )

                record = {
                    "purchase_id":      str(uuid.uuid4()),
                    "user_id":          user_id,
                    "session_id":       s["session_id"],
                    "purchase_time":    maybe_corrupt_timestamp(isoformat_z(purchase_time)),
                    "item_name":        item["item_name"],
                    "item_type":        item["item_type"],
                    "price_usd":        item["price_usd"],
                    "currency":         "USD",
                    "country":          s["country"],
                    "device":           s["device"],
                    "payment_provider": weighted_choice([
                        ("apple_pay",    0.30),
                        ("google_play",  0.45),
                        ("credit_card",  0.20),
                        ("paypal",       0.05),
                    ]),
                    "purchase_status":  weighted_choice([
                        ("completed", 0.96),
                        ("failed",    0.03),
                        ("refunded",  0.01),
                    ]),
                }

                # intentional bad values
                if random.random() < 0.004:
                    record["price_usd"] = -1.0
                if random.random() < 0.003:
                    record["currency"] = "US"
                if random.random() < 0.003:
                    record["item_name"] = None

                purchases.append(record)

    # ~0.5% duplicate rows
    if purchases:
        purchases.extend(random.sample(purchases, k=max(1, len(purchases) // 200)))

    return purchases


def load_players() -> list:
    spark = get_spark()
    df    = spark.read.parquet(f"{BASE_OUTPUT_DIR}/player_profiles")
    return [row.asDict() for row in df.collect()]


def load_sessions_by_user() -> dict:
    spark = get_spark()
    df    = spark.read.parquet(f"{BASE_OUTPUT_DIR}/sessions")
    sessions_by_user = {}
    for row in df.collect():
        rec = row.asDict()
        sessions_by_user.setdefault(rec["user_id"], []).append(rec)
    return sessions_by_user


def main():
    print("Loading player profiles...")
    players = load_players()

    print("Loading sessions...")
    sessions_by_user = load_sessions_by_user()

    print("Generating purchases...")
    purchases = generate_purchases(players, sessions_by_user)
    purchases = add_date_partition_col(purchases, "purchase_time")

    write_parquet(
        purchases,
        f"{BASE_OUTPUT_DIR}/purchases",
        partition_by="event_date",
    )

    print(f"Done. Purchases written: {len(purchases):,}")
    return purchases


if __name__ == "__main__":
    main()

import os
import json
import uuid
import random
import hashlib
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed()

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
BASE_OUTPUT_DIR = "data/raw"
NUM_PLAYERS = 5000
START_DATE = datetime(2026, 1, 1)
END_DATE = datetime(2026, 3, 31)

COUNTRIES = ["FI", "SE", "NO", "DK", "DE", "GB", "US", "CA", "NG", "IN"]
DEVICES = ["ios", "android"]
ACQUISITION_CHANNELS = ["organic", "facebook_ads", "google_ads", "tiktok_ads", "referral"]
EVENT_TYPES = [
    "game_install",
    "tutorial_start",
    "tutorial_complete",
    "session_start",
    "session_end",
    "level_start",
    "level_complete",
    "level_fail",
    "purchase"
]

ITEM_CATALOG = [
    {"item_name": "gold_pack_small", "item_type": "currency", "price_usd": 1.99},
    {"item_name": "gold_pack_medium", "item_type": "currency", "price_usd": 4.99},
    {"item_name": "gold_pack_large", "item_type": "currency", "price_usd": 9.99},
    {"item_name": "starter_bundle", "item_type": "bundle", "price_usd": 3.99},
    {"item_name": "battle_pass", "item_type": "subscription", "price_usd": 7.99},
    {"item_name": "energy_refill", "item_type": "consumable", "price_usd": 0.99},
    {"item_name": "epic_skin", "item_type": "cosmetic", "price_usd": 12.99},
]

LEVELS = list(range(1, 21))

# ---------------------------------------------------
# STABLE ATTRIBUTE HELPERS
# These use a hash of the user_id so the same user always
# gets the same value regardless of when the script is run.
# ---------------------------------------------------
def stable_choice(user_id: str, options: list, salt: str = "") -> str:
    """Always returns the same option for a given user_id + salt combo."""
    key = f"{user_id}{salt}".encode()
    hash_int = int(hashlib.md5(key).hexdigest(), 16)
    return options[hash_int % len(options)]

def stable_weighted_choice(user_id: str, options_with_weights: list, salt: str = ""):
    """
    Deterministic weighted choice for a given user_id + salt.
    options_with_weights: list of (value, weight) tuples.
    """
    key = f"{user_id}{salt}".encode()
    hash_int = int(hashlib.md5(key).hexdigest(), 16)
    # Use hash to produce a stable float in [0, 1)
    stable_float = (hash_int % 1_000_000) / 1_000_000

    values = [x[0] for x in options_with_weights]
    weights = [x[1] for x in options_with_weights]
    total = sum(weights)
    cumulative = 0.0
    for value, weight in zip(values, weights):
        cumulative += weight / total
        if stable_float < cumulative:
            return value
    return values[-1]

# ---------------------------------------------------
# HELPERS
# ---------------------------------------------------
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def random_datetime(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = int(delta.total_seconds())
    return start + timedelta(seconds=random.randint(0, seconds))

def isoformat_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def daterange(start_date: datetime, end_date: datetime):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)

def weighted_choice(options_with_weights):
    values = [x[0] for x in options_with_weights]
    weights = [x[1] for x in options_with_weights]
    return random.choices(values, weights=weights, k=1)[0]

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

def write_json_lines(path: str, records: list):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

# ---------------------------------------------------
# PLAYER PROFILE GENERATION
# Stable attributes (country, device, acquisition_channel,
# skill_tier, payer_segment, engagement_segment) are derived
# deterministically from user_id so they never change between runs.
# Everything else (names, install dates, flags) can vary.
# ---------------------------------------------------
def generate_player_profiles(num_players=NUM_PLAYERS):
    players = []

    for i in range(1, num_players + 1):
        user_id = f"u{i:06d}"
        install_dt = random_datetime(START_DATE, END_DATE)

        # --- STABLE: same every run for this user_id ---
        country = stable_choice(user_id, COUNTRIES, salt="country")
        device = stable_weighted_choice(user_id, [("android", 0.62), ("ios", 0.38)], salt="device")
        acquisition_channel = stable_weighted_choice(user_id, [
            ("organic", 0.35),
            ("facebook_ads", 0.20),
            ("google_ads", 0.18),
            ("tiktok_ads", 0.17),
            ("referral", 0.10)
        ], salt="acquisition")
        skill_tier = stable_weighted_choice(user_id, [
            ("low", 0.25),
            ("mid", 0.50),
            ("high", 0.25)
        ], salt="skill")
        payer_segment = stable_weighted_choice(user_id, [
            ("non_payer", 0.70),
            ("light_payer", 0.20),
            ("whale", 0.10)
        ], salt="payer")
        engagement_segment = stable_weighted_choice(user_id, [
            ("casual", 0.40),
            ("regular", 0.45),
            ("hardcore", 0.15)
        ], salt="engagement")
        app_version = stable_weighted_choice(user_id, [
            ("1.0.0", 0.10),
            ("1.1.0", 0.20),
            ("1.2.0", 0.30),
            ("1.3.0", 0.25),
            ("1.4.0", 0.15)
        ], salt="appversion")

        # --- VARIABLE: changes each run ---
        record = {
            "user_id": user_id,
            "player_name": fake.user_name(),
            "install_date": install_dt.strftime("%Y-%m-%d"),
            "install_ts": isoformat_z(install_dt),
            "country": maybe_corrupt_country(country),
            "device": maybe_corrupt_device(device),
            "app_version": app_version,
            "acquisition_channel": acquisition_channel,
            "skill_tier": skill_tier,
            "payer_segment": payer_segment,
            "engagement_segment": engagement_segment,
            "marketing_opt_in": random.choice([True, False]),
            "created_at": isoformat_z(install_dt)
        }

        # intentional messiness (still random — affects different users each run)
        if random.random() < 0.01:
            record["country"] = None
        if random.random() < 0.01:
            record["device"] = None
        if random.random() < 0.005:
            record["install_date"] = None

        players.append(record)

    # add duplicate profile rows
    duplicates = random.sample(players, k=max(1, len(players) // 100))
    players.extend(duplicates)

    return players

# ---------------------------------------------------
# SESSION GENERATION
# ---------------------------------------------------
def generate_sessions(players):
    sessions = []
    sessions_by_user = {}

    for p in players:
        user_id = p["user_id"]

        if user_id in sessions_by_user:
            continue

        install_date_str = p.get("install_date")
        if not install_date_str:
            sessions_by_user[user_id] = []
            continue

        install_dt = datetime.strptime(install_date_str, "%Y-%m-%d")
        end_dt = END_DATE

        days_since_install = (end_dt - install_dt).days
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
                (1, 0.65),
                (2, 0.25),
                (3, 0.08),
                (4, 0.02)
            ])

            for _ in range(num_sessions_today):
                start_hour = random.randint(7, 23)
                start_min = random.randint(0, 59)
                start_sec = random.randint(0, 59)
                session_start = active_day.replace(
                    hour=start_hour, minute=start_min, second=start_sec
                )

                duration_minutes = weighted_choice([
                    (random.randint(3, 10), 0.30),
                    (random.randint(10, 25), 0.45),
                    (random.randint(25, 45), 0.20),
                    (random.randint(45, 90), 0.05)
                ])
                session_end = session_start + timedelta(minutes=duration_minutes)

                # Use the player's stable country/device as the base,
                # then apply corruption on top (corruption is still random per run)
                base_device = p["device"] if p.get("device") else random.choice(DEVICES)
                base_country = p["country"] if p.get("country") else random.choice(COUNTRIES)

                session_record = {
                    "session_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "session_start": maybe_corrupt_timestamp(isoformat_z(session_start)),
                    "session_end": maybe_corrupt_timestamp(isoformat_z(session_end)),
                    "session_duration_seconds": duration_minutes * 60,
                    "device": maybe_corrupt_device(base_device),
                    "country": maybe_corrupt_country(base_country),
                    "app_version": p["app_version"]
                }

                if random.random() < 0.005:
                    session_record["session_duration_seconds"] = -1
                if random.random() < 0.003:
                    session_record["session_end"] = None

                sessions.append(session_record)
                user_sessions.append(session_record)

        sessions_by_user[user_id] = user_sessions

    if sessions:
        sessions.extend(random.sample(sessions, k=max(1, len(sessions) // 200)))

    return sessions, sessions_by_user

# ---------------------------------------------------
# GAME EVENT GENERATION
# ---------------------------------------------------
def generate_game_events(players, sessions_by_user):
    events = []
    seen_users = set()

    for p in players:
        user_id = p["user_id"]
        if user_id in seen_users:
            continue
        seen_users.add(user_id)

        install_ts = p.get("install_ts")
        if install_ts:
            install_event = {
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "event_time": maybe_corrupt_timestamp(install_ts),
                "event_type": "game_install",
                "session_id": None,
                "level_id": None,
                "attempt_number": None,
                "duration_seconds": None,
                "result": None,
                "country": maybe_corrupt_country(p["country"]) if p.get("country") else None,
                "device": maybe_corrupt_device(p["device"]) if p.get("device") else None,
                "app_version": p["app_version"]
            }
            events.append(install_event)

            if random.random() < 0.9:
                tutorial_start_time = datetime.strptime(install_ts, "%Y-%m-%dT%H:%M:%SZ") + timedelta(minutes=1)
                events.append({
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "event_time": maybe_corrupt_timestamp(isoformat_z(tutorial_start_time)),
                    "event_type": "tutorial_start",
                    "session_id": None,
                    "level_id": 0,
                    "attempt_number": 1,
                    "duration_seconds": None,
                    "result": None,
                    "country": p.get("country"),
                    "device": p.get("device"),
                    "app_version": p["app_version"]
                })

                if random.random() < 0.82:
                    tutorial_complete_time = tutorial_start_time + timedelta(minutes=random.randint(2, 8))
                    events.append({
                        "event_id": str(uuid.uuid4()),
                        "user_id": user_id,
                        "event_time": maybe_corrupt_timestamp(isoformat_z(tutorial_complete_time)),
                        "event_type": "tutorial_complete",
                        "session_id": None,
                        "level_id": 0,
                        "attempt_number": 1,
                        "duration_seconds": random.randint(90, 500),
                        "result": "success",
                        "country": p.get("country"),
                        "device": p.get("device"),
                        "app_version": p["app_version"]
                    })

        skill = p["skill_tier"]
        user_sessions = sessions_by_user.get(user_id, [])
        current_level = 1

        for s in user_sessions:
            session_id = s["session_id"]

            events.append({
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "event_time": s["session_start"],
                "event_type": "session_start",
                "session_id": session_id,
                "level_id": None,
                "attempt_number": None,
                "duration_seconds": None,
                "result": None,
                "country": s["country"],
                "device": s["device"],
                "app_version": s["app_version"]
            })

            num_levels_attempted = weighted_choice([
                (1, 0.40),
                (2, 0.30),
                (3, 0.20),
                (4, 0.08),
                (5, 0.02)
            ])

            base_time = None
            try:
                base_time = datetime.strptime(
                    s["session_start"].replace(" ", "T").replace("Z", ""),
                    "%Y-%m-%dT%H:%M:%S"
                )
            except Exception:
                base_time = random_datetime(START_DATE, END_DATE)

            for _ in range(num_levels_attempted):
                if current_level > max(LEVELS):
                    current_level = max(LEVELS)

                start_time = base_time + timedelta(minutes=random.randint(0, 15))
                attempt_no = 1

                events.append({
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "event_time": maybe_corrupt_timestamp(isoformat_z(start_time)),
                    "event_type": "level_start",
                    "session_id": session_id,
                    "level_id": current_level,
                    "attempt_number": attempt_no,
                    "duration_seconds": None,
                    "result": None,
                    "country": s["country"],
                    "device": s["device"],
                    "app_version": s["app_version"]
                })

                level_difficulty_penalty = min(current_level * 0.015, 0.25)
                if skill == "high":
                    success_prob = 0.82 - level_difficulty_penalty
                elif skill == "mid":
                    success_prob = 0.65 - level_difficulty_penalty
                else:
                    success_prob = 0.48 - level_difficulty_penalty

                success_prob = max(0.15, success_prob)
                result_time = start_time + timedelta(seconds=random.randint(60, 500))

                if random.random() < success_prob:
                    events.append({
                        "event_id": str(uuid.uuid4()),
                        "user_id": user_id,
                        "event_time": maybe_corrupt_timestamp(isoformat_z(result_time)),
                        "event_type": "level_complete",
                        "session_id": session_id,
                        "level_id": current_level,
                        "attempt_number": attempt_no,
                        "duration_seconds": int((result_time - start_time).total_seconds()),
                        "result": "success",
                        "country": s["country"],
                        "device": s["device"],
                        "app_version": s["app_version"]
                    })
                    if current_level < max(LEVELS):
                        current_level += 1
                else:
                    events.append({
                        "event_id": str(uuid.uuid4()),
                        "user_id": user_id,
                        "event_time": maybe_corrupt_timestamp(isoformat_z(result_time)),
                        "event_type": "level_fail",
                        "session_id": session_id,
                        "level_id": current_level,
                        "attempt_number": attempt_no,
                        "duration_seconds": int((result_time - start_time).total_seconds()),
                        "result": "fail",
                        "country": s["country"],
                        "device": s["device"],
                        "app_version": s["app_version"]
                    })

            events.append({
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "event_time": s["session_end"],
                "event_type": "session_end",
                "session_id": session_id,
                "level_id": None,
                "attempt_number": None,
                "duration_seconds": s["session_duration_seconds"],
                "result": None,
                "country": s["country"],
                "device": s["device"],
                "app_version": s["app_version"]
            })

    if events:
        events.extend(random.sample(events, k=max(1, len(events) // 250)))

    return events

# ---------------------------------------------------
# PURCHASE GENERATION
# ---------------------------------------------------
def generate_purchases(players, sessions_by_user):
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
            purchase_probability_per_session = 0.01
        elif payer_segment == "light_payer":
            purchase_probability_per_session = 0.06
        else:
            purchase_probability_per_session = 0.18

        for s in user_sessions:
            if random.random() < purchase_probability_per_session:
                num_purchases = weighted_choice([
                    (1, 0.85),
                    (2, 0.13),
                    (3, 0.02)
                ])

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
                        seconds=random.randint(0, 59)
                    )

                    purchase_record = {
                        "purchase_id": str(uuid.uuid4()),
                        "user_id": user_id,
                        "session_id": s["session_id"],
                        "purchase_time": maybe_corrupt_timestamp(isoformat_z(purchase_time)),
                        "item_name": item["item_name"],
                        "item_type": item["item_type"],
                        "price_usd": item["price_usd"],
                        "currency": "USD",
                        "country": s["country"],
                        "device": s["device"],
                        "payment_provider": weighted_choice([
                            ("apple_pay", 0.30),
                            ("google_play", 0.45),
                            ("credit_card", 0.20),
                            ("paypal", 0.05)
                        ]),
                        "purchase_status": weighted_choice([
                            ("completed", 0.96),
                            ("failed", 0.03),
                            ("refunded", 0.01)
                        ])
                    }

                    if random.random() < 0.004:
                        purchase_record["price_usd"] = -1.0
                    if random.random() < 0.003:
                        purchase_record["currency"] = "US"
                    if random.random() < 0.003:
                        purchase_record["item_name"] = None

                    purchases.append(purchase_record)

    if purchases:
        purchases.extend(random.sample(purchases, k=max(1, len(purchases) // 200)))

    return purchases

# ---------------------------------------------------
# FILE WRITERS
# ---------------------------------------------------
def write_partitioned_by_date(records, base_dir, timestamp_field, filename_prefix):
    grouped = {}

    for rec in records:
        ts = rec.get(timestamp_field)
        if not ts or ts == "INVALID_TIMESTAMP":
            partition_date = "unknown/unknown/unknown"
        else:
            try:
                normalized = ts.replace(" ", "T").replace("Z", "")
                dt = datetime.strptime(normalized, "%Y-%m-%dT%H:%M:%S")
                partition_date = f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}"
            except Exception:
                partition_date = "unknown/unknown/unknown"

        grouped.setdefault(partition_date, []).append(rec)

    for partition, rows in grouped.items():
        out_path = os.path.join(base_dir, partition, f"{filename_prefix}.json")
        write_json_lines(out_path, rows)

# ---------------------------------------------------
# MAIN
# ---------------------------------------------------
def main():
    print("Generating player profiles...")
    players = generate_player_profiles(NUM_PLAYERS)

    print("Generating sessions...")
    sessions, sessions_by_user = generate_sessions(players)

    print("Generating game events...")
    game_events = generate_game_events(players, sessions_by_user)

    print("Generating purchases...")
    purchases = generate_purchases(players, sessions_by_user)

    print("Writing player profiles...")
    write_json_lines(
        os.path.join(BASE_OUTPUT_DIR, "player_profiles", "profiles_001.json"),
        players
    )

    print("Writing sessions...")
    write_partitioned_by_date(
        sessions,
        os.path.join(BASE_OUTPUT_DIR, "sessions"),
        "session_start",
        "sessions_001"
    )

    print("Writing game events...")
    write_partitioned_by_date(
        game_events,
        os.path.join(BASE_OUTPUT_DIR, "game_events"),
        "event_time",
        "events_001"
    )

    print("Writing purchases...")
    write_partitioned_by_date(
        purchases,
        os.path.join(BASE_OUTPUT_DIR, "purchases"),
        "purchase_time",
        "purchases_001"
    )

    print("\nDone.")
    print(f"Players:      {len(players):,}")
    print(f"Sessions:     {len(sessions):,}")
    print(f"Game events:  {len(game_events):,}")
    print(f"Purchases:    {len(purchases):,}")

if __name__ == "__main__":
    main()

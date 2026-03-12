"""
generate_events.py
------------------
Generates game events for each player session and writes them to:
    data/raw/game_events/<year>/<month>/<day>/events_001.json

Event types produced:
    game_install, tutorial_start, tutorial_complete,
    session_start, session_end,
    level_start, level_complete, level_fail

Depends on:
    generate_players.py   (reads profiles_001.json if run standalone)
    generate_sessions.py  (reads sessions partitions if run standalone)
"""

import uuid
import random
from datetime import datetime, timedelta

from common import (
    BASE_OUTPUT_DIR,
    LEVELS,
    START_DATE,
    END_DATE,
    weighted_choice,
    random_datetime,
    isoformat_z,
    maybe_corrupt_timestamp,
    maybe_corrupt_device,
    maybe_corrupt_country,
    get_spark,
    write_parquet,
    add_date_partition_col,
)


def generate_game_events(players: list, sessions_by_user: dict) -> list:
    events = []
    seen_users = set()

    for p in players:
        user_id = p["user_id"]
        if user_id in seen_users:
            continue
        seen_users.add(user_id)

        # --- install + tutorial events ---
        install_ts = p.get("install_ts")
        if install_ts:
            events.append({
                "event_id":         str(uuid.uuid4()),
                "user_id":          user_id,
                "event_time":       maybe_corrupt_timestamp(install_ts),
                "event_type":       "game_install",
                "session_id":       None,
                "level_id":         None,
                "attempt_number":   None,
                "duration_seconds": None,
                "result":           None,
                "country":          maybe_corrupt_country(p["country"]) if p.get("country") else None,
                "device":           maybe_corrupt_device(p["device"])   if p.get("device")  else None,
                "app_version":      p["app_version"],
            })

            if random.random() < 0.9:
                t_start = datetime.strptime(install_ts, "%Y-%m-%dT%H:%M:%SZ") + timedelta(minutes=1)
                events.append({
                    "event_id":         str(uuid.uuid4()),
                    "user_id":          user_id,
                    "event_time":       maybe_corrupt_timestamp(isoformat_z(t_start)),
                    "event_type":       "tutorial_start",
                    "session_id":       None,
                    "level_id":         0,
                    "attempt_number":   1,
                    "duration_seconds": None,
                    "result":           None,
                    "country":          p.get("country"),
                    "device":           p.get("device"),
                    "app_version":      p["app_version"],
                })

                if random.random() < 0.82:
                    t_end = t_start + timedelta(minutes=random.randint(2, 8))
                    events.append({
                        "event_id":         str(uuid.uuid4()),
                        "user_id":          user_id,
                        "event_time":       maybe_corrupt_timestamp(isoformat_z(t_end)),
                        "event_type":       "tutorial_complete",
                        "session_id":       None,
                        "level_id":         0,
                        "attempt_number":   1,
                        "duration_seconds": random.randint(90, 500),
                        "result":           "success",
                        "country":          p.get("country"),
                        "device":           p.get("device"),
                        "app_version":      p["app_version"],
                    })

        # --- session + gameplay events ---
        skill = p["skill_tier"]
        current_level = 1

        for s in sessions_by_user.get(user_id, []):
            session_id = s["session_id"]

            events.append({
                "event_id":         str(uuid.uuid4()),
                "user_id":          user_id,
                "event_time":       s["session_start"],
                "event_type":       "session_start",
                "session_id":       session_id,
                "level_id":         None,
                "attempt_number":   None,
                "duration_seconds": None,
                "result":           None,
                "country":          s["country"],
                "device":           s["device"],
                "app_version":      s["app_version"],
            })

            # parse session start for timing gameplay events
            try:
                base_time = datetime.strptime(
                    s["session_start"].replace(" ", "T").replace("Z", ""),
                    "%Y-%m-%dT%H:%M:%S"
                )
            except Exception:
                base_time = random_datetime(START_DATE, END_DATE)

            num_levels_attempted = weighted_choice([
                (1, 0.40), (2, 0.30), (3, 0.20), (4, 0.08), (5, 0.02)
            ])

            for _ in range(num_levels_attempted):
                if current_level > max(LEVELS):
                    current_level = max(LEVELS)

                start_time = base_time + timedelta(minutes=random.randint(0, 15))

                events.append({
                    "event_id":         str(uuid.uuid4()),
                    "user_id":          user_id,
                    "event_time":       maybe_corrupt_timestamp(isoformat_z(start_time)),
                    "event_type":       "level_start",
                    "session_id":       session_id,
                    "level_id":         current_level,
                    "attempt_number":   1,
                    "duration_seconds": None,
                    "result":           None,
                    "country":          s["country"],
                    "device":           s["device"],
                    "app_version":      s["app_version"],
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
                duration_s  = int((result_time - start_time).total_seconds())

                if random.random() < success_prob:
                    events.append({
                        "event_id":         str(uuid.uuid4()),
                        "user_id":          user_id,
                        "event_time":       maybe_corrupt_timestamp(isoformat_z(result_time)),
                        "event_type":       "level_complete",
                        "session_id":       session_id,
                        "level_id":         current_level,
                        "attempt_number":   1,
                        "duration_seconds": duration_s,
                        "result":           "success",
                        "country":          s["country"],
                        "device":           s["device"],
                        "app_version":      s["app_version"],
                    })
                    if current_level < max(LEVELS):
                        current_level += 1
                else:
                    events.append({
                        "event_id":         str(uuid.uuid4()),
                        "user_id":          user_id,
                        "event_time":       maybe_corrupt_timestamp(isoformat_z(result_time)),
                        "event_type":       "level_fail",
                        "session_id":       session_id,
                        "level_id":         current_level,
                        "attempt_number":   1,
                        "duration_seconds": duration_s,
                        "result":           "fail",
                        "country":          s["country"],
                        "device":           s["device"],
                        "app_version":      s["app_version"],
                    })

            events.append({
                "event_id":         str(uuid.uuid4()),
                "user_id":          user_id,
                "event_time":       s["session_end"],
                "event_type":       "session_end",
                "session_id":       session_id,
                "level_id":         None,
                "attempt_number":   None,
                "duration_seconds": s["session_duration_seconds"],
                "result":           None,
                "country":          s["country"],
                "device":           s["device"],
                "app_version":      s["app_version"],
            })

    # ~0.4% duplicate rows
    if events:
        events.extend(random.sample(events, k=max(1, len(events) // 250)))

    return events


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

    print("Generating game events...")
    events = generate_game_events(players, sessions_by_user)
    events = add_date_partition_col(events, "event_time")

    write_parquet(
        events,
        f"{BASE_OUTPUT_DIR}/game_events",
        partition_by="event_date",
    )

    print(f"Done. Events written: {len(events):,}")
    return events


if __name__ == "__main__":
    main()

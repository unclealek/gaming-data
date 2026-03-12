"""
generate_players.py
-------------------
Generates player profiles and writes them to:
    data/raw/player_profiles/profiles_001.json

Stable per user_id (same every run):
    country, device, acquisition_channel, skill_tier,
    payer_segment, engagement_segment, app_version

Varies each run:
    player_name, install_date, install_ts, marketing_opt_in
"""

import random

from common import (
    BASE_OUTPUT_DIR,
    NUM_PLAYERS,
    START_DATE,
    END_DATE,
    COUNTRIES,
    DEVICES,
    fake,
    stable_choice,
    stable_weighted_choice,
    random_datetime,
    isoformat_z,
    maybe_corrupt_device,
    maybe_corrupt_country,
    write_parquet,
)


def generate_player_profiles(num_players: int = NUM_PLAYERS) -> list:
    players = []

    for i in range(1, num_players + 1):
        user_id = f"u{i:06d}"
        install_dt = random_datetime(START_DATE, END_DATE)

        # --- STABLE: same every run for this user_id ---
        country = stable_choice(user_id, COUNTRIES, salt="country")
        device = stable_weighted_choice(
            user_id, [("android", 0.62), ("ios", 0.38)], salt="device"
        )
        acquisition_channel = stable_weighted_choice(user_id, [
            ("organic",      0.35),
            ("facebook_ads", 0.20),
            ("google_ads",   0.18),
            ("tiktok_ads",   0.17),
            ("referral",     0.10),
        ], salt="acquisition")
        skill_tier = stable_weighted_choice(user_id, [
            ("low",  0.25),
            ("mid",  0.50),
            ("high", 0.25),
        ], salt="skill")
        payer_segment = stable_weighted_choice(user_id, [
            ("non_payer",   0.70),
            ("light_payer", 0.20),
            ("whale",       0.10),
        ], salt="payer")
        engagement_segment = stable_weighted_choice(user_id, [
            ("casual",   0.40),
            ("regular",  0.45),
            ("hardcore", 0.15),
        ], salt="engagement")
        app_version = stable_weighted_choice(user_id, [
            ("1.0.0", 0.10),
            ("1.1.0", 0.20),
            ("1.2.0", 0.30),
            ("1.3.0", 0.25),
            ("1.4.0", 0.15),
        ], salt="appversion")

        # --- VARIABLE: changes each run ---
        record = {
            "user_id":            user_id,
            "player_name":        fake.user_name(),
            "install_date":       install_dt.strftime("%Y-%m-%d"),
            "install_ts":         isoformat_z(install_dt),
            "country":            maybe_corrupt_country(country),
            "device":             maybe_corrupt_device(device),
            "app_version":        app_version,
            "acquisition_channel": acquisition_channel,
            "skill_tier":         skill_tier,
            "payer_segment":      payer_segment,
            "engagement_segment": engagement_segment,
            "marketing_opt_in":   random.choice([True, False]),
            "created_at":         isoformat_z(install_dt),
        }

        # intentional messiness
        if random.random() < 0.01:
            record["country"] = None
        if random.random() < 0.01:
            record["device"] = None
        if random.random() < 0.005:
            record["install_date"] = None

        players.append(record)

    # add ~1% duplicate rows
    duplicates = random.sample(players, k=max(1, len(players) // 100))
    players.extend(duplicates)

    return players


def main():
    print("Generating player profiles...")
    players = generate_player_profiles(NUM_PLAYERS)

    write_parquet(players, f"{BASE_OUTPUT_DIR}/player_profiles")

    print(f"Done. Players written: {len(players):,}")
    return players


if __name__ == "__main__":
    main()

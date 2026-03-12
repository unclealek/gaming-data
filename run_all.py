"""
run_all.py
----------
Runs all four generators in the correct dependency order:

    1. generate_players   (no dependencies)
    2. generate_sessions  (needs players)
    3. generate_events    (needs players + sessions)
    4. generate_purchases (needs players + sessions)

Data is passed in memory between steps — no intermediate disk reads.

Run this as a Databricks Job or in a notebook cell:
    %run ./run_all
"""

from generate_players   import generate_player_profiles
from generate_sessions  import generate_sessions
from generate_events    import generate_game_events
from generate_purchases import generate_purchases

from common import (
    BASE_OUTPUT_DIR,
    NUM_PLAYERS,
    write_parquet,
    add_date_partition_col,
)


def main():
    # 1. Players
    print("=" * 45)
    print("Step 1/4 — Player profiles")
    print("=" * 45)
    players = generate_player_profiles(NUM_PLAYERS)
    write_parquet(players, f"{BASE_OUTPUT_DIR}/player_profiles")

    # 2. Sessions
    print("\n" + "=" * 45)
    print("Step 2/4 — Sessions")
    print("=" * 45)
    sessions, sessions_by_user = generate_sessions(players)
    sessions = add_date_partition_col(sessions, "session_start")
    write_parquet(sessions, f"{BASE_OUTPUT_DIR}/sessions", partition_by="event_date")

    # 3. Game events
    print("\n" + "=" * 45)
    print("Step 3/4 — Game events")
    print("=" * 45)
    events = generate_game_events(players, sessions_by_user)
    events = add_date_partition_col(events, "event_time")
    write_parquet(events, f"{BASE_OUTPUT_DIR}/game_events", partition_by="event_date")

    # 4. Purchases
    print("\n" + "=" * 45)
    print("Step 4/4 — Purchases")
    print("=" * 45)
    purchases = generate_purchases(players, sessions_by_user)
    purchases = add_date_partition_col(purchases, "purchase_time")
    write_parquet(purchases, f"{BASE_OUTPUT_DIR}/purchases", partition_by="event_date")

    # Summary
    print("\n" + "=" * 45)
    print("All done.")
    print(f"  Players:   {len(players):,}")
    print(f"  Sessions:  {len(sessions):,}")
    print(f"  Events:    {len(events):,}")
    print(f"  Purchases: {len(purchases):,}")
    print(f"\n  Output:    {BASE_OUTPUT_DIR}/")
    print("=" * 45)


main()

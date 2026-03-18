"""Játékos azonosítás (reconciliation) engine.

Fázisok:
1. Shotchart playercode → player regisztráció
2. Shotchart + scoresheet bridge: name match → license_number ↔ playercode
3. PBP name → playercode feloldás
4. Feloldatlan játékosok riport
"""

import sqlite3
from .normalize import normalize_name, names_match, name_similarity


def resolve_players(conn: sqlite3.Connection, report: bool = False):
    """Multi-phase player reconciliation."""

    print("🔗 Játékos azonosítás indítása...\n")

    # Phase 1: Register players from shotchart data (has playercode)
    _phase1_shotchart_players(conn)

    # Phase 2: Bridge scoresheet (license_number) ↔ shotchart (playercode)
    _phase2_scoresheet_bridge(conn)

    # Phase 3: Resolve PBP player names
    _phase3_pbp_names(conn)

    # Phase 4: Update playercode in fact tables
    _phase4_update_fact_tables(conn)

    conn.commit()

    # Summary
    _print_summary(conn, report)


def _phase1_shotchart_players(conn: sqlite3.Connection):
    """Register all unique playercode+name pairs from shots table."""
    rows = conn.execute(
        """SELECT DISTINCT playercode, player_name
           FROM shots
           WHERE playercode IS NOT NULL AND playercode != ''"""
    ).fetchall()

    count = 0
    for r in rows:
        pc = r["playercode"]
        name = r["player_name"]
        if not pc or not name:
            continue

        conn.execute(
            """INSERT INTO players (playercode, canonical_name)
               VALUES (?, ?)
               ON CONFLICT(playercode) DO NOTHING""",
            (pc, name),
        )
        conn.execute(
            """INSERT OR IGNORE INTO player_names (playercode, name_variant, source)
               VALUES (?, ?, 'shotchart')""",
            (pc, name),
        )
        count += 1

    print(f"  Phase 1: {count} játékos regisztrálva shotchart-ból")


def _phase2_scoresheet_bridge(conn: sqlite3.Connection):
    """Bridge: match shots playercode+name with scoresheet license_number+name."""

    # Get all matches that have BOTH scoresheet and shotchart data
    matches = conn.execute(
        "SELECT gamecode FROM matches WHERE has_scoresheet = 1 AND has_shotchart = 1"
    ).fetchall()

    links_made = 0
    for m in matches:
        gc = m["gamecode"]

        # Get scoresheet players for this match
        ss_players = conn.execute(
            """SELECT DISTINCT player_name, license_number, team
               FROM player_game_stats
               WHERE gamecode = ? AND license_number IS NOT NULL
                 AND source IN ('scoresheet', 'merged')""",
            (gc,),
        ).fetchall()

        # Get shot players for this match
        shot_players = conn.execute(
            """SELECT DISTINCT playercode, player_name, team_id
               FROM shots
               WHERE gamecode = ? AND playercode IS NOT NULL""",
            (gc,),
        ).fetchall()

        # For each scoresheet player, find best shotchart match
        for ss in ss_players:
            best_pc = None
            best_score = 0.0

            for sh in shot_players:
                score = name_similarity(ss["player_name"], sh["player_name"])
                if score > best_score and score >= 0.7:
                    best_score = score
                    best_pc = sh["playercode"]

            if best_pc and ss["license_number"]:
                # Link license_number → playercode
                existing = conn.execute(
                    "SELECT license_number FROM players WHERE playercode = ?",
                    (best_pc,),
                ).fetchone()

                if existing and not existing["license_number"]:
                    conn.execute(
                        "UPDATE players SET license_number = ? WHERE playercode = ?",
                        (ss["license_number"], best_pc),
                    )
                    links_made += 1

                # Register name variant
                conn.execute(
                    """INSERT OR IGNORE INTO player_names
                       (playercode, name_variant, source)
                       VALUES (?, ?, 'scoresheet')""",
                    (best_pc, ss["player_name"]),
                )

    print(f"  Phase 2: {links_made} license_number ↔ playercode link létrehozva")


def _phase3_pbp_names(conn: sqlite3.Connection):
    """Resolve PBP player names to playercodes via name matching."""

    # Get all PBP players without playercode
    pbp_players = conn.execute(
        """SELECT DISTINCT gamecode, player_name, team
           FROM player_game_stats
           WHERE playercode IS NULL AND source IN ('pbp', 'merged')"""
    ).fetchall()

    # Get all known player names
    known = conn.execute(
        "SELECT playercode, name_variant FROM player_names"
    ).fetchall()

    resolved = 0
    for p in pbp_players:
        best_pc = None
        best_score = 0.0

        for k in known:
            score = name_similarity(p["player_name"], k["name_variant"])
            if score > best_score and score >= 0.8:
                best_score = score
                best_pc = k["playercode"]

        if best_pc:
            conn.execute(
                """UPDATE player_game_stats SET playercode = ?
                   WHERE gamecode = ? AND team = ? AND player_name = ?""",
                (best_pc, p["gamecode"], p["team"], p["player_name"]),
            )
            conn.execute(
                """INSERT OR IGNORE INTO player_names
                   (playercode, name_variant, source)
                   VALUES (?, ?, 'pbp')""",
                (best_pc, p["player_name"]),
            )
            resolved += 1

    print(f"  Phase 3: {resolved} PBP játékos feloldva")


def _phase4_update_fact_tables(conn: sqlite3.Connection):
    """Update playercode in player_game_stats using license_number lookups."""

    # Update via license_number
    updated = conn.execute(
        """UPDATE player_game_stats SET playercode = (
             SELECT p.playercode FROM players p
             WHERE p.license_number = player_game_stats.license_number
           )
           WHERE playercode IS NULL
             AND license_number IS NOT NULL
             AND EXISTS (
               SELECT 1 FROM players p
               WHERE p.license_number = player_game_stats.license_number
             )"""
    ).rowcount

    # Update via name match from player_names
    updated2 = conn.execute(
        """UPDATE player_game_stats SET playercode = (
             SELECT pn.playercode FROM player_names pn
             WHERE pn.name_variant = player_game_stats.player_name
             LIMIT 1
           )
           WHERE playercode IS NULL
             AND EXISTS (
               SELECT 1 FROM player_names pn
               WHERE pn.name_variant = player_game_stats.player_name
             )"""
    ).rowcount

    print(f"  Phase 4: {updated + updated2} player_game_stats sor frissítve playercode-dal")


def _print_summary(conn: sqlite3.Connection, report: bool):
    """Print reconciliation summary."""
    total_players = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
    with_license = conn.execute(
        "SELECT COUNT(*) FROM players WHERE license_number IS NOT NULL"
    ).fetchone()[0]

    total_pgs = conn.execute(
        "SELECT COUNT(DISTINCT player_name) FROM player_game_stats"
    ).fetchone()[0]
    resolved_pgs = conn.execute(
        "SELECT COUNT(DISTINCT player_name) FROM player_game_stats WHERE playercode IS NOT NULL"
    ).fetchone()[0]

    print(f"\n--- Összegzés ---")
    print(f"  Regisztrált játékosok: {total_players}")
    print(f"  License number-rel:    {with_license}")
    print(
        f"  Player stats feloldva: {resolved_pgs}/{total_pgs} "
        f"({resolved_pgs/total_pgs*100:.0f}%)" if total_pgs else ""
    )

    if report:
        unresolved = conn.execute(
            """SELECT DISTINCT player_name, gamecode, team, source
               FROM player_game_stats
               WHERE playercode IS NULL
               ORDER BY player_name"""
        ).fetchall()

        if unresolved:
            print(f"\n⚠️  Feloldatlan játékosok ({len(unresolved)} sor):")
            seen = set()
            for r in unresolved:
                name = r["player_name"]
                if name not in seen:
                    print(f"    {name} ({r['source']}, {r['gamecode']})")
                    seen.add(name)

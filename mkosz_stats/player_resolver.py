"""Játékos azonosítás (reconciliation) engine.

Egyszerűsített fázisok (nincs scoresheet↔shotchart bridge, mert nincs overlap):
1. Shotchart playercode → player regisztráció
2. NB1B PBP ↔ shotchart bridge: azonos meccsen name match → playercode
3. Exact name propagáció: ismert nevek más meccsekre
4. Fact table update: playercode kitöltés license_number és name alapján
"""

import sqlite3
from .normalize import normalize_name, names_match, name_similarity


def resolve_players(conn: sqlite3.Connection, report: bool = False):
    """Multi-phase player reconciliation."""

    print("🔗 Játékos azonosítás indítása...\n")

    # Phase 1: Register players from shotchart data (has playercode)
    _phase1_shotchart_players(conn)

    # Phase 2: NB1B PBP ↔ shotchart bridge (azonos meccsen name match)
    _phase2_pbp_shotchart_bridge(conn)

    # Phase 3: Exact name propagáció más meccsekre
    _phase3_name_propagation(conn)

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


def _phase2_pbp_shotchart_bridge(conn: sqlite3.Connection):
    """NB1B bridge: azonos meccsen PBP player_name ↔ shotchart wbname → playercode.

    Csak olyan meccsekre ahol PBP és shotchart is van (NB1B).
    """
    matches = conn.execute(
        "SELECT gamecode FROM matches WHERE has_pbp = 1 AND has_shotchart = 1"
    ).fetchall()

    resolved = 0
    for m in matches:
        gc = m["gamecode"]

        # PBP players without playercode
        pbp_players = conn.execute(
            """SELECT DISTINCT player_name, team
               FROM player_game_stats
               WHERE gamecode = ? AND playercode IS NULL AND source = 'pbp'""",
            (gc,),
        ).fetchall()

        # Shotchart players with playercode
        shot_players = conn.execute(
            """SELECT DISTINCT playercode, player_name
               FROM shots
               WHERE gamecode = ? AND playercode IS NOT NULL""",
            (gc,),
        ).fetchall()

        for pbp in pbp_players:
            best_pc = None
            best_score = 0.0

            for sh in shot_players:
                score = name_similarity(pbp["player_name"], sh["player_name"])
                if score > best_score and score >= 0.7:
                    best_score = score
                    best_pc = sh["playercode"]

            if best_pc:
                conn.execute(
                    """UPDATE player_game_stats SET playercode = ?
                       WHERE gamecode = ? AND team = ? AND player_name = ?""",
                    (best_pc, gc, pbp["team"], pbp["player_name"]),
                )
                conn.execute(
                    """INSERT OR IGNORE INTO player_names
                       (playercode, name_variant, source)
                       VALUES (?, ?, 'pbp')""",
                    (best_pc, pbp["player_name"]),
                )
                resolved += 1

    print(f"  Phase 2: {resolved} PBP játékos feloldva (NB1B bridge)")


def _phase3_name_propagation(conn: sqlite3.Connection):
    """Propagate known playercode-name pairs to other matches.

    Ha egy player_name már ismert a player_names táblából,
    más meccseken is kitöltjük a playercode-ot.
    """
    updated = conn.execute(
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

    print(f"  Phase 3: {updated} player_game_stats sor frissítve (name propagáció)")


def _phase4_update_fact_tables(conn: sqlite3.Connection):
    """Update playercode via license_number lookups (ha van bridge)."""

    # Update via license_number (jövőben ha roster scrape kitölti)
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

    if updated:
        print(f"  Phase 4: {updated} sor frissítve license_number alapján")
    else:
        print(f"  Phase 4: 0 sor (nincs license_number↔playercode link még)")


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
    if with_license:
        print(f"  License number-rel:    {with_license}")
    if total_pgs:
        print(
            f"  Player stats feloldva: {resolved_pgs}/{total_pgs} "
            f"({resolved_pgs/total_pgs*100:.0f}%)"
        )

    if report:
        # Only show PBP/shotchart unresolved (scoresheet players won't have playercode)
        unresolved = conn.execute(
            """SELECT DISTINCT player_name, gamecode, team, source
               FROM player_game_stats
               WHERE playercode IS NULL AND source IN ('pbp', 'merged')
               ORDER BY player_name"""
        ).fetchall()

        if unresolved:
            print(f"\n⚠️  Feloldatlan PBP játékosok ({len(unresolved)} sor):")
            seen = set()
            for r in unresolved:
                name = r["player_name"]
                if name not in seen:
                    print(f"    {name} ({r['source']}, {r['gamecode']})")
                    seen.add(name)
        else:
            print(f"\n✅ Minden PBP játékos feloldva!")

        # Scoresheet coverage (ezeknek nincs playercode, de van license_number)
        ss_total = conn.execute(
            "SELECT COUNT(DISTINCT player_name) FROM player_game_stats WHERE source = 'scoresheet'"
        ).fetchone()[0]
        if ss_total:
            print(f"\n📋 Scoresheet játékosok: {ss_total} (license_number alapú, nincs playercode)")

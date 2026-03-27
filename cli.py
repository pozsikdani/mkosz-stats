#!/usr/bin/env python3
"""MKOSZ Stats — egységes kosárlabda statisztikai adatbázis CLI."""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DB_PATH, SCORESHEET_DB, PBP_DB
from mkosz_stats.db import init_db, get_connection, db_status


def cmd_init(args):
    """Adatbázis inicializálás."""
    db = args.db or DB_PATH
    init_db(db)


def cmd_import(args):
    """Import forrásokból."""
    db = args.db or DB_PATH
    conn = get_connection(db)

    if args.source == "scoresheet":
        from mkosz_stats.importers.scoresheet import import_scoresheet

        src = args.path or SCORESHEET_DB
        if not os.path.exists(src):
            print(f"HIBA: Forrás DB nem található: {src}")
            sys.exit(1)
        import_scoresheet(conn, src, season=args.season or "x2526")

    elif args.source == "pbp":
        from mkosz_stats.importers.pbp import import_pbp

        src = args.path or PBP_DB
        if not os.path.exists(src):
            print(f"HIBA: Forrás DB nem található: {src}")
            sys.exit(1)
        import_pbp(conn, src)

    elif args.source == "web":
        from mkosz_stats.importers.web import import_web

        import_web(conn, comp=args.comp, season=args.season or "x2526")

    elif args.source == "shotchart":
        from mkosz_stats.importers.shotchart import import_shotchart

        if args.all_matches:
            import_shotchart(conn, all_matches=True)
        elif args.comp and args.team:
            import_shotchart(
                conn,
                comp=args.comp,
                season=args.season or "x2526",
                team_id=args.team,
            )
        else:
            print("HIBA: --all vagy --comp + --team szükséges")
            sys.exit(1)

    conn.close()


def cmd_resolve(args):
    """Játékos azonosítás futtatás."""
    db = args.db or DB_PATH
    conn = get_connection(db)

    from mkosz_stats.player_resolver import resolve_players

    resolve_players(conn, report=args.report)
    conn.close()


def cmd_status(args):
    """DB státusz kiírás."""
    db = args.db or DB_PATH
    if not os.path.exists(db):
        print(f"Adatbázis nem található: {db}")
        sys.exit(1)
    conn = get_connection(db)
    counts = db_status(conn)

    print("=" * 50)
    print("MKOSZ Stats — Adatbázis összegzés")
    print("=" * 50)

    # Fő számok
    print(f"\n📊 Meccsek:        {counts.get('matches', 0)}")
    print(f"👤 Játékosok:      {counts.get('players', 0)}")
    print(f"🏀 Csapatok:       {counts.get('teams', 0)}")
    print(f"🏆 Bajnokságok:    {counts.get('competitions', 0)}")

    # Adat coverage
    if counts.get("matches", 0):
        row = conn.execute(
            "SELECT SUM(has_scoresheet), SUM(has_pbp), SUM(has_shotchart) FROM matches"
        ).fetchone()
        sc, pbp, shot = row[0] or 0, row[1] or 0, row[2] or 0
        total = counts["matches"]
        print(f"\n📋 Scoresheet:     {sc}/{total} ({sc/total*100:.0f}%)")
        print(f"📝 Play-by-play:   {pbp}/{total} ({pbp/total*100:.0f}%)")
        print(f"🎯 Shotchart:      {shot}/{total} ({shot/total*100:.0f}%)")

    # Részletes táblák
    print(f"\n--- Részletes tábla méretek ---")
    for table, count in sorted(counts.items()):
        if count > 0:
            print(f"  {table:25s} {count:>8,}")

    # Feloldatlan játékosok
    unresolved = conn.execute(
        "SELECT COUNT(DISTINCT player_name) FROM player_game_stats WHERE playercode IS NULL"
    ).fetchone()[0]
    total_players = conn.execute(
        "SELECT COUNT(DISTINCT player_name) FROM player_game_stats"
    ).fetchone()[0]
    if total_players:
        resolved = total_players - unresolved
        print(
            f"\n🔗 Játékos azonosítás: {resolved}/{total_players} "
            f"({resolved/total_players*100:.0f}%) feloldva"
        )
        if unresolved:
            print(f"   ⚠️  {unresolved} feloldatlan játékos")

    conn.close()


def cmd_player(args):
    """Játékos összefoglaló."""
    db = args.db or DB_PATH
    conn = get_connection(db)

    from mkosz_stats.queries.player import player_summary

    player_summary(conn, args.query)
    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="MKOSZ Stats — egységes kosárlabda statisztikai adatbázis"
    )
    parser.add_argument("--db", help="DB fájl útvonal (default: mkosz_stats.sqlite)")
    sub = parser.add_subparsers(dest="command")

    # init
    sub.add_parser("init", help="Adatbázis inicializálás")

    # import
    p_import = sub.add_parser("import", help="Adat importálás")
    p_import.add_argument(
        "source", choices=["scoresheet", "pbp", "shotchart", "web"], help="Forrás típus"
    )
    p_import.add_argument("path", nargs="?", help="Forrás DB útvonal")
    p_import.add_argument("--season", default="x2526")
    p_import.add_argument("--comp", help="Bajnokság kód (shotchart-hoz)")
    p_import.add_argument("--team", help="Team ID (shotchart-hoz)")
    p_import.add_argument(
        "--all", dest="all_matches", action="store_true",
        help="Minden DB-beli meccsre (shotchart)"
    )

    # resolve
    p_resolve = sub.add_parser("resolve", help="Játékos azonosítás")
    p_resolve.add_argument(
        "--report", action="store_true", help="Feloldatlan játékosok listázása"
    )

    # status
    sub.add_parser("status", help="DB státusz")

    # player
    p_player = sub.add_parser("player", help="Játékos összefoglaló")
    p_player.add_argument("query", help="Playercode (A777201) vagy név")

    args = parser.parse_args()

    if args.command == "init":
        cmd_init(args)
    elif args.command == "import":
        cmd_import(args)
    elif args.command == "resolve":
        cmd_resolve(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "player":
        cmd_player(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

"""Játékos lekérdezések — cross-source összefoglaló."""

import sqlite3
from ..normalize import normalize_name


def player_summary(conn: sqlite3.Connection, query: str):
    """Játékos összefoglaló kiírás playercode vagy név alapján."""

    # Find player
    player = None
    if query.startswith(("A", "B")) and len(query) >= 5:
        # Try as playercode
        player = conn.execute(
            "SELECT * FROM players WHERE playercode = ?", (query,)
        ).fetchone()

    if not player:
        # Search by name
        rows = conn.execute("SELECT * FROM players").fetchall()
        norm_q = normalize_name(query)
        for r in rows:
            if norm_q in normalize_name(r["canonical_name"]):
                player = r
                break

    if not player:
        # Search in player_game_stats directly
        rows = conn.execute(
            "SELECT DISTINCT player_name, playercode FROM player_game_stats"
        ).fetchall()
        norm_q = normalize_name(query)
        for r in rows:
            if norm_q in normalize_name(r["player_name"]):
                if r["playercode"]:
                    player = conn.execute(
                        "SELECT * FROM players WHERE playercode = ?",
                        (r["playercode"],),
                    ).fetchone()
                else:
                    # No playercode, show what we have
                    _show_unresolved_player(conn, r["player_name"])
                    return
                break

    if not player:
        print(f"Játékos nem található: {query}")
        return

    pc = player["playercode"]
    print("=" * 60)
    print(f"  {player['canonical_name']}")
    print(f"  Playercode: {pc}")
    if player["license_number"]:
        print(f"  Igazolásszám: {player['license_number']}")
    if player["birth_year"]:
        print(f"  Születési év: {player['birth_year']}")
    if player["height_cm"]:
        print(f"  Magasság: {player['height_cm']} cm")
    if player["position"]:
        print(f"  Pozíció: {player['position']}")
    print("=" * 60)

    # Name variants
    names = conn.execute(
        "SELECT name_variant, source FROM player_names WHERE playercode = ?", (pc,)
    ).fetchall()
    if names:
        print(f"\n  Név variánsok:")
        for n in names:
            print(f"    {n['name_variant']} ({n['source']})")

    # Game stats
    games = conn.execute(
        """SELECT pgs.*, m.match_date, m.team_a_name, m.team_b_name, m.comp_code
           FROM player_game_stats pgs
           JOIN matches m ON pgs.gamecode = m.gamecode
           WHERE pgs.playercode = ? OR pgs.player_name IN (
             SELECT name_variant FROM player_names WHERE playercode = ?
           )
           ORDER BY m.match_date""",
        (pc, pc),
    ).fetchall()

    if games:
        print(f"\n  📊 Meccsek ({len(games)}):")
        print(f"  {'Dátum':12s} {'Meccs':40s} {'PTS':>4s} {'2FG':>4s} {'3FG':>4s} {'FT':>6s} {'±':>4s} {'Forrás':>10s}")
        print("  " + "-" * 85)

        total_pts = 0
        total_games = 0
        for g in games:
            date = g["match_date"] or "?"
            opp = g["team_b_name"] if g["team"] == "A" else g["team_a_name"]
            prefix = "vs" if g["team"] == "A" else "@"
            match_str = f"{prefix} {opp}"[:40]

            pts = g["points"] or 0
            fg2 = g["fg2_made"] if g["fg2_made"] is not None else "-"
            fg3 = g["fg3_made"] if g["fg3_made"] is not None else "-"
            ft = f"{g['ft_made'] or 0}/{g['ft_attempted'] or 0}" if g["ft_attempted"] else "-"
            pm = g["plus_minus"] if g["plus_minus"] is not None else "-"

            print(f"  {date:12s} {match_str:40s} {pts:>4} {fg2:>4} {fg3:>4} {ft:>6} {str(pm):>4} {g['source']:>10s}")

            total_pts += pts
            total_games += 1

        if total_games:
            print(f"\n  Átlag: {total_pts / total_games:.1f} pont/meccs ({total_games} meccs)")

    # Shots
    shot_count = conn.execute(
        "SELECT COUNT(*) FROM shots WHERE playercode = ?", (pc,)
    ).fetchone()[0]
    if shot_count:
        shot_stats = conn.execute(
            """SELECT zone, COUNT(*) as total,
                      SUM(is_made) as made
               FROM shots
               WHERE playercode = ? AND is_free_throw = 0
               GROUP BY zone""",
            (pc,),
        ).fetchall()

        print(f"\n  🎯 Dobástérkép ({shot_count} dobás):")
        for s in shot_stats:
            pct = s["made"] / s["total"] * 100 if s["total"] else 0
            print(f"    {s['zone']:8s} {s['made']}/{s['total']} ({pct:.0f}%)")


def _show_unresolved_player(conn: sqlite3.Connection, player_name: str):
    """Show info for a player without playercode."""
    print(f"⚠️  Feloldatlan játékos: {player_name}")
    print(f"   (Nincs playercode — futtasd: python3 cli.py resolve)")

    games = conn.execute(
        """SELECT pgs.*, m.match_date, m.team_a_name, m.team_b_name
           FROM player_game_stats pgs
           JOIN matches m ON pgs.gamecode = m.gamecode
           WHERE pgs.player_name = ?
           ORDER BY m.match_date""",
        (player_name,),
    ).fetchall()

    if games:
        print(f"\n  Meccsek ({len(games)}):")
        for g in games:
            opp = g["team_b_name"] if g["team"] == "A" else g["team_a_name"]
            print(f"    {g['match_date'] or '?':12s} vs {opp} — {g['points'] or 0} pont ({g['source']})")

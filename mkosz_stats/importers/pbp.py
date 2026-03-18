"""Play-by-play import: pbp.sqlite → unified DB."""

import sqlite3
from ..team_resolver import ensure_competition, ensure_season
from config import COMPETITIONS


def import_pbp(conn: sqlite3.Connection, src_path: str):
    """Import all data from PBP SQLite database."""
    src = sqlite3.connect(src_path)
    src.row_factory = sqlite3.Row

    # ── 1. Import matches ──────────────────────────────────────────
    matches = src.execute(
        "SELECT match_id, comp_code, season, comp_name, round_name, "
        "match_date, match_time, venue, team_a, team_b, "
        "team_a_full, team_b_full, score_a, score_b, "
        "quarter_scores, referees FROM matches"
    ).fetchall()

    match_count = 0
    for m in matches:
        gamecode = m["match_id"]  # Already in gamecode format
        season = m["season"]
        comp_code = m["comp_code"]

        ensure_season(conn, season)
        comp_info = COMPETITIONS.get(comp_code)
        if comp_info:
            ensure_competition(conn, comp_code, season, *comp_info)
        else:
            ensure_competition(conn, comp_code, season, m["comp_name"])

        conn.execute(
            """INSERT INTO matches
               (gamecode, comp_code, season, round_name,
                match_date, match_time, venue,
                team_a_name, team_b_name,
                score_a, score_b, quarter_scores, referees,
                has_pbp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
               ON CONFLICT(gamecode) DO UPDATE SET
                 round_name = COALESCE(excluded.round_name, matches.round_name),
                 match_date = COALESCE(excluded.match_date, matches.match_date),
                 match_time = COALESCE(excluded.match_time, matches.match_time),
                 venue = COALESCE(excluded.venue, matches.venue),
                 quarter_scores = COALESCE(excluded.quarter_scores, matches.quarter_scores),
                 referees = COALESCE(excluded.referees, matches.referees),
                 has_pbp = 1""",
            (
                gamecode, comp_code, season, m["round_name"],
                m["match_date"], m["match_time"], m["venue"],
                m["team_a_full"] or m["team_a"],
                m["team_b_full"] or m["team_b"],
                m["score_a"], m["score_b"],
                m["quarter_scores"], m["referees"],
            ),
        )
        match_count += 1

    print(f"  ✅ {match_count} meccs importálva (PBP)")

    # ── 2. Import player_stats → player_game_stats ─────────────────
    stats = src.execute(
        "SELECT match_id, team, player_name, is_starter, minutes, "
        "plus_minus, val, ts_pct, efg_pct, game_score, "
        "usg_pct, ast_to, tov_pct FROM player_stats"
    ).fetchall()

    # PBP-nek nincsenek alap stats mezői (points, fg stb.) a player_stats-ban,
    # azok az events-ből aggregálhatók — de az importnál a haladó statokat mentjük
    stats_count = 0
    for s in stats:
        gamecode = s["match_id"]

        # Aggregate basic stats from events for this player+match
        basic = _aggregate_basic_stats(src, gamecode, s["team"], s["player_name"])

        conn.execute(
            """INSERT INTO player_game_stats
               (gamecode, team, player_name, is_starter,
                points, fg2_made, fg3_made, ft_made, ft_attempted,
                oreb, dreb, assists, steals, turnovers, blocks,
                blocks_against, fouls_drawn, personal_fouls,
                minutes, plus_minus, val,
                ts_pct, efg_pct, game_score, usg_pct, ast_to, tov_pct,
                source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pbp')
               ON CONFLICT(gamecode, team, player_name) DO UPDATE SET
                 is_starter = COALESCE(excluded.is_starter, player_game_stats.is_starter),
                 minutes = excluded.minutes,
                 oreb = excluded.oreb,
                 dreb = excluded.dreb,
                 assists = excluded.assists,
                 steals = excluded.steals,
                 turnovers = excluded.turnovers,
                 blocks = excluded.blocks,
                 blocks_against = excluded.blocks_against,
                 fouls_drawn = excluded.fouls_drawn,
                 plus_minus = excluded.plus_minus,
                 val = excluded.val,
                 ts_pct = excluded.ts_pct,
                 efg_pct = excluded.efg_pct,
                 game_score = excluded.game_score,
                 usg_pct = excluded.usg_pct,
                 ast_to = excluded.ast_to,
                 tov_pct = excluded.tov_pct,
                 source = CASE
                   WHEN player_game_stats.source = 'scoresheet' THEN 'merged'
                   ELSE excluded.source
                 END""",
            (
                gamecode, s["team"], s["player_name"], s["is_starter"],
                basic["points"], basic["fg2_made"], basic["fg3_made"],
                basic["ft_made"], basic["ft_attempted"],
                basic["oreb"], basic["dreb"], basic["assists"],
                basic["steals"], basic["turnovers"], basic["blocks"],
                basic["blocks_against"], basic["fouls_drawn"],
                basic["personal_fouls"],
                s["minutes"], s["plus_minus"], s["val"],
                s["ts_pct"], s["efg_pct"], s["game_score"],
                s["usg_pct"], s["ast_to"], s["tov_pct"],
            ),
        )
        stats_count += 1

    print(f"  ✅ {stats_count} player_game_stats sor importálva (PBP)")

    # ── 3. Import events → pbp_events ──────────────────────────────
    events = src.execute(
        "SELECT match_id, event_seq, quarter, minute, team, "
        "player_name, event_type, event_raw, counter, "
        "score_a, score_b, is_scoring, points FROM events"
    ).fetchall()

    ev_count = 0
    for e in events:
        conn.execute(
            """INSERT OR IGNORE INTO pbp_events
               (gamecode, event_seq, quarter, minute, team,
                player_name, event_type, event_raw, counter,
                score_a, score_b, is_scoring, points)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                e["match_id"], e["event_seq"], e["quarter"], e["minute"],
                e["team"], e["player_name"], e["event_type"], e["event_raw"],
                e["counter"], e["score_a"], e["score_b"],
                e["is_scoring"], e["points"],
            ),
        )
        ev_count += 1

    print(f"  ✅ {ev_count} pbp_events sor importálva")

    # ── 4. Import substitutions ────────────────────────────────────
    subs = src.execute(
        "SELECT match_id, event_seq, quarter, minute, team, "
        "player_in, player_out FROM substitutions"
    ).fetchall()

    sub_count = 0
    for s in subs:
        conn.execute(
            """INSERT INTO substitutions
               (gamecode, event_seq, quarter, minute, team,
                player_in_name, player_out_name)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                s["match_id"], s["event_seq"], s["quarter"], s["minute"],
                s["team"], s["player_in"], s["player_out"],
            ),
        )
        sub_count += 1

    print(f"  ✅ {sub_count} substitution importálva")

    # ── 5. Import timeouts ─────────────────────────────────────────
    tos = src.execute(
        "SELECT match_id, event_seq, quarter, minute, team FROM timeouts"
    ).fetchall()

    to_count = 0
    for t in tos:
        conn.execute(
            """INSERT INTO timeouts
               (gamecode, event_seq, quarter, minute, team, source)
               VALUES (?, ?, ?, ?, ?, 'pbp')""",
            (t["match_id"], t["event_seq"], t["quarter"], t["minute"], t["team"]),
        )
        to_count += 1

    print(f"  ✅ {to_count} timeout importálva (PBP)")

    conn.commit()
    src.close()
    print(f"\n📝 PBP import kész: {match_count} meccs")


def _aggregate_basic_stats(
    src: sqlite3.Connection, match_id: str, team: str, player_name: str
) -> dict:
    """Aggregate basic stats from PBP events for a player."""
    events = src.execute(
        "SELECT event_type, points FROM events "
        "WHERE match_id = ? AND team = ? AND player_name = ?",
        (match_id, team, player_name),
    ).fetchall()

    stats = {
        "points": 0,
        "fg2_made": 0, "fg3_made": 0,
        "ft_made": 0, "ft_attempted": 0,
        "oreb": 0, "dreb": 0,
        "assists": 0, "steals": 0, "turnovers": 0,
        "blocks": 0, "blocks_against": 0,
        "fouls_drawn": 0, "personal_fouls": 0,
    }

    for e in events:
        et = e["event_type"]
        pts = e["points"] or 0

        if et in ("CLOSE_MADE", "MID_MADE", "DUNK_MADE"):
            stats["fg2_made"] += 1
            stats["points"] += 2
        elif et == "THREE_MADE":
            stats["fg3_made"] += 1
            stats["points"] += 3
        elif et == "FT_MADE":
            stats["ft_made"] += 1
            stats["ft_attempted"] += 1
            stats["points"] += 1
        elif et == "FT_MISS":
            stats["ft_attempted"] += 1
        elif et == "OREB":
            stats["oreb"] += 1
        elif et == "DREB":
            stats["dreb"] += 1
        elif et == "AST":
            stats["assists"] += 1
        elif et == "STL":
            stats["steals"] += 1
        elif et == "TOV":
            stats["turnovers"] += 1
        elif et == "BLK":
            stats["blocks"] += 1
        elif et == "BLK_RECV":
            stats["blocks_against"] += 1
        elif et == "FOUL":
            stats["personal_fouls"] += 1
        elif et == "FOUL_DRAWN":
            stats["fouls_drawn"] += 1

    return stats

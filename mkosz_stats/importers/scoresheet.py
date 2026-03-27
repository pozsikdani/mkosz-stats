"""Scoresheet import: scoresheet.sqlite → unified DB."""

import sqlite3
from ..normalize import extract_gamecode_from_pdf, split_gamecode
from ..team_resolver import ensure_competition, ensure_season
from config import SCORESHEET_PREFIX_TO_COMP, COMPETITIONS


def import_scoresheet(conn: sqlite3.Connection, src_path: str, season: str = "x2526"):
    """Import all data from scoresheet SQLite database."""
    src = sqlite3.connect(src_path)
    src.row_factory = sqlite3.Row

    ensure_season(conn, season)

    # ── 1. Import matches ──────────────────────────────────────────
    matches = src.execute(
        "SELECT match_id, source_pdf, team_a, team_b, score_a, score_b, "
        "match_date, match_time, venue, winner FROM matches"
    ).fetchall()

    match_count = 0
    gamecode_map = {}  # scoresheet match_id → gamecode

    for m in matches:
        gamecode = extract_gamecode_from_pdf(m["source_pdf"])
        if not gamecode:
            print(f"  ⚠️  Nincs gamecode: {m['match_id']} ({m['source_pdf']})")
            continue

        comp_code, game_id = split_gamecode(gamecode)
        gamecode_map[m["match_id"]] = gamecode

        # Ensure competition exists
        comp_info = COMPETITIONS.get(comp_code)
        if comp_info:
            ensure_competition(conn, comp_code, season, *comp_info)
        else:
            ensure_competition(conn, comp_code, season)

        conn.execute(
            """INSERT INTO matches
               (gamecode, comp_code, season, match_date, match_time, venue,
                team_a_name, team_b_name, score_a, score_b,
                has_scoresheet, scoresheet_match_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
               ON CONFLICT(gamecode) DO UPDATE SET
                 match_date = COALESCE(excluded.match_date, matches.match_date),
                 match_time = COALESCE(excluded.match_time, matches.match_time),
                 venue = COALESCE(excluded.venue, matches.venue),
                 team_a_name = excluded.team_a_name,
                 team_b_name = excluded.team_b_name,
                 score_a = excluded.score_a,
                 score_b = excluded.score_b,
                 has_scoresheet = 1,
                 scoresheet_match_id = excluded.scoresheet_match_id""",
            (
                gamecode, comp_code, season,
                m["match_date"], m["match_time"], m["venue"],
                m["team_a"], m["team_b"],
                m["score_a"], m["score_b"],
                m["match_id"],
            ),
        )
        match_count += 1

    print(f"  ✅ {match_count} meccs importálva")

    # ── 2. Import player_game_stats ────────────────────────────────
    stats = src.execute(
        "SELECT match_id, team, jersey_number, license_number, name, "
        "points, fg2_made, fg3_made, ft_made, ft_attempted, "
        "personal_fouls, starter, entry_quarter FROM player_game_stats"
    ).fetchall()

    stats_count = 0
    for s in stats:
        gamecode = gamecode_map.get(s["match_id"])
        if not gamecode:
            continue

        conn.execute(
            """INSERT INTO player_game_stats
               (gamecode, team, player_name, license_number, jersey_number,
                is_starter, entry_quarter, points,
                fg2_made, fg3_made, ft_made, ft_attempted,
                personal_fouls, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'scoresheet')
               ON CONFLICT(gamecode, team, player_name) DO UPDATE SET
                 license_number = COALESCE(excluded.license_number, player_game_stats.license_number),
                 jersey_number = COALESCE(excluded.jersey_number, player_game_stats.jersey_number),
                 points = excluded.points,
                 fg2_made = excluded.fg2_made,
                 fg3_made = excluded.fg3_made,
                 ft_made = excluded.ft_made,
                 ft_attempted = excluded.ft_attempted,
                 personal_fouls = excluded.personal_fouls""",
            (
                gamecode, s["team"], s["name"],
                s["license_number"], s["jersey_number"],
                s["starter"] or 0, s["entry_quarter"],
                s["points"],
                s["fg2_made"], s["fg3_made"],
                s["ft_made"], s["ft_attempted"],
                s["personal_fouls"],
            ),
        )
        stats_count += 1

    print(f"  ✅ {stats_count} player_game_stats sor importálva")

    # ── 3. Import scoring_events ───────────────────────────────────
    events = src.execute(
        "SELECT match_id, event_seq, quarter, minute, team, "
        "jersey_number, license_number, points, shot_type, made, "
        "score_a, score_b FROM scoring_events"
    ).fetchall()

    ev_count = 0
    for e in events:
        gamecode = gamecode_map.get(e["match_id"])
        if not gamecode:
            continue

        conn.execute(
            """INSERT OR IGNORE INTO scoring_events
               (gamecode, event_seq, quarter, minute, team,
                jersey_number, license_number, points, shot_type, made,
                score_a, score_b)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                gamecode, e["event_seq"], e["quarter"], e["minute"],
                e["team"], e["jersey_number"], e["license_number"],
                e["points"], e["shot_type"], e["made"],
                e["score_a"], e["score_b"],
            ),
        )
        ev_count += 1

    print(f"  ✅ {ev_count} scoring_events sor importálva")

    # ── 4. Import quarter_scores ───────────────────────────────────
    qs = src.execute(
        "SELECT match_id, quarter, score_a, score_b FROM quarter_scores"
    ).fetchall()

    qs_count = 0
    for q in qs:
        gamecode = gamecode_map.get(q["match_id"])
        if not gamecode:
            continue
        conn.execute(
            """INSERT OR IGNORE INTO quarter_scores
               (gamecode, quarter, score_a, score_b)
               VALUES (?, ?, ?, ?)""",
            (gamecode, q["quarter"], q["score_a"], q["score_b"]),
        )
        qs_count += 1

    print(f"  ✅ {qs_count} quarter_scores sor importálva")

    # ── 5. Import timeouts ─────────────────────────────────────────
    to = src.execute(
        "SELECT match_id, team, quarter, minute FROM timeouts"
    ).fetchall()

    to_count = 0
    for t in to:
        gamecode = gamecode_map.get(t["match_id"])
        if not gamecode:
            continue
        conn.execute(
            """INSERT INTO timeouts (gamecode, quarter, minute, team, source)
               VALUES (?, ?, ?, ?, 'scoresheet')""",
            (gamecode, t["quarter"], t["minute"], t["team"]),
        )
        to_count += 1

    print(f"  ✅ {to_count} timeout importálva")

    # ── 6. Import personal_fouls ───────────────────────────────────
    pf = src.execute(
        "SELECT match_id, team, jersey_number, foul_number, minute, "
        "quarter, foul_type, foul_category, free_throws, offsetting "
        "FROM personal_fouls"
    ).fetchall()

    pf_count = 0
    for f in pf:
        gamecode = gamecode_map.get(f["match_id"])
        if not gamecode:
            continue
        conn.execute(
            """INSERT INTO personal_fouls
               (gamecode, team, jersey_number, foul_number, minute,
                quarter, foul_type, foul_category, free_throws, offsetting)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                gamecode, f["team"], f["jersey_number"],
                f["foul_number"], f["minute"], f["quarter"],
                f["foul_type"], f["foul_category"],
                f["free_throws"], f["offsetting"],
            ),
        )
        pf_count += 1

    print(f"  ✅ {pf_count} personal_fouls sor importálva")

    conn.commit()
    src.close()
    print(f"\n📋 Scoresheet import kész: {match_count} meccs")

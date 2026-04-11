"""Play-by-play import: pbp.sqlite → unified DB."""

import json
import sqlite3
import unicodedata
from ..team_resolver import ensure_competition, ensure_season
from config import COMPETITIONS


def _normalize_for_match(name):
    """Strip accents and encoding artifacts to ASCII for name comparison."""
    for src, dst in [("Ő", "O"), ("Ű", "U"), ("ő", "o"), ("ű", "u"),
                     ("Õ", "O"), ("õ", "o"), ("Û", "U"), ("û", "u")]:
        name = name.replace(src, dst)
    # Replace ? with O (encoding artifact — usually ő→O, occasionally ű→U, but O works for matching)
    name = name.replace("?", "O")
    nfkd = unicodedata.normalize("NFKD", name)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).upper()


def _names_match(name_a, name_b):
    """Check if two player names refer to the same person.

    Handles: encoding artifacts (?/õ/û), accent differences, name truncation.
    Compares word-by-word: all words in the shorter name must appear as prefixes
    in the longer name's words.
    """
    na = _normalize_for_match(name_a).split()
    nb = _normalize_for_match(name_b).split()
    if not na or not nb:
        return False
    # Family name (first word) must match — allow prefix match for encoding artifacts
    # e.g. GERCS (from GER?CS) should match GEROCS (from GERŐCS)
    if not (na[0].startswith(nb[0]) or nb[0].startswith(na[0])):
        return False
    # The shorter set of given names must all appear as prefixes in the longer set
    short, long = (na[1:], nb[1:]) if len(na) <= len(nb) else (nb[1:], na[1:])
    if not short:
        return True  # Only family name — match
    for sw in short:
        if not any(lw.startswith(sw) or sw.startswith(lw) for lw in long):
            return False
    return True


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
                 score_a = CASE WHEN excluded.score_a > 0 THEN excluded.score_a ELSE matches.score_a END,
                 score_b = CASE WHEN excluded.score_b > 0 THEN excluded.score_b ELSE matches.score_b END,
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

    # ── 2. Aggregate player_game_stats from events ────────────────
    # Discover all players who appear in events for each match+team
    player_rows = src.execute(
        "SELECT DISTINCT match_id, team, player_name FROM events "
        "WHERE player_name IS NOT NULL"
    ).fetchall()

    stats_count = 0
    for pr in player_rows:
        gamecode = pr["match_id"]
        team = pr["team"]
        player_name_raw = pr["player_name"]
        player_name = player_name_raw.upper()

        # Match to scoresheet name if possible (handles truncated names + encoding artifacts)
        scoresheet_rows = conn.execute(
            "SELECT player_name FROM player_game_stats WHERE gamecode=? AND team=? AND source='scoresheet'",
            (gamecode, team),
        ).fetchall()
        for sr in scoresheet_rows:
            if _names_match(player_name, sr[0]):
                player_name = sr[0]
                break

        basic = _aggregate_basic_stats(src, gamecode, team, player_name_raw)
        is_starter = _is_starter(src, gamecode, team, player_name_raw)
        minutes = _get_player_minutes(src, gamecode, team, player_name_raw, is_starter)

        conn.execute(
            """INSERT INTO player_game_stats
               (gamecode, team, player_name, is_starter,
                points, fg2_made, fg2_attempted, fg3_made, fg3_attempted,
                ft_made, ft_attempted,
                oreb, dreb, assists, steals, turnovers, blocks,
                blocks_against, fouls_drawn, personal_fouls,
                minutes, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pbp')
               ON CONFLICT(gamecode, team, player_name) DO UPDATE SET
                 is_starter = COALESCE(excluded.is_starter, player_game_stats.is_starter),
                 fg2_made = excluded.fg2_made,
                 fg2_attempted = excluded.fg2_attempted,
                 fg3_made = excluded.fg3_made,
                 fg3_attempted = excluded.fg3_attempted,
                 ft_made = excluded.ft_made,
                 ft_attempted = excluded.ft_attempted,
                 minutes = excluded.minutes,
                 oreb = excluded.oreb,
                 dreb = excluded.dreb,
                 assists = excluded.assists,
                 steals = excluded.steals,
                 turnovers = excluded.turnovers,
                 blocks = excluded.blocks,
                 blocks_against = excluded.blocks_against,
                 fouls_drawn = excluded.fouls_drawn,
                 source = CASE
                   WHEN player_game_stats.source = 'scoresheet' THEN 'merged'
                   ELSE excluded.source
                 END""",
            (
                gamecode, team, player_name, int(is_starter),
                basic["points"], basic["fg2_made"], basic["fg2_attempted"],
                basic["fg3_made"], basic["fg3_attempted"],
                basic["ft_made"], basic["ft_attempted"],
                basic["oreb"], basic["dreb"], basic["assists"],
                basic["steals"], basic["turnovers"], basic["blocks"],
                basic["blocks_against"], basic["fouls_drawn"],
                basic["personal_fouls"],
                minutes,
            ),
        )
        stats_count += 1

    print(f"  ✅ {stats_count} player_game_stats sor importálva (PBP)")

    # ── 2b. Propagate license_number from scoresheet to PBP rows ────
    scoresheet_ids = conn.execute(
        "SELECT DISTINCT player_name, license_number FROM player_game_stats "
        "WHERE source = 'scoresheet' AND license_number IS NOT NULL"
    ).fetchall()

    # Exact name match (fast)
    exact_count = 0
    for ss_name, lic in scoresheet_ids:
        updated = conn.execute(
            "UPDATE player_game_stats SET license_number = ? "
            "WHERE license_number IS NULL AND player_name = ?",
            (lic, ss_name),
        ).rowcount
        exact_count += updated

    # Fuzzy match for encoding differences (GER?CS vs GERŐCS)
    fuzzy_count = 0
    pbp_nulls = conn.execute(
        "SELECT DISTINCT player_name FROM player_game_stats WHERE license_number IS NULL"
    ).fetchall()
    for (pbp_name,) in pbp_nulls:
        for ss_name, lic in scoresheet_ids:
            if _names_match(pbp_name, ss_name):
                updated = conn.execute(
                    "UPDATE player_game_stats SET license_number = ? "
                    "WHERE license_number IS NULL AND player_name = ?",
                    (lic, pbp_name),
                ).rowcount
                fuzzy_count += updated
                break

    conn.commit()
    if exact_count or fuzzy_count:
        print(f"  ✅ license_number propagálva: {exact_count} exact + {fuzzy_count} fuzzy")

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
                e["team"], e["player_name"].upper() if e["player_name"] else None, e["event_type"], e["event_raw"],
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
                s["team"], s["player_in"].upper(), s["player_out"].upper(),
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
    """Aggregate basic stats from PBP events for a player.

    Counts both made AND missed field goals for FG% calculation (Fix 5).
    """
    events = src.execute(
        "SELECT event_type, points FROM events "
        "WHERE match_id = ? AND team = ? AND player_name = ?",
        (match_id, team, player_name),
    ).fetchall()

    stats = {
        "points": 0,
        "fg2_made": 0, "fg2_attempted": 0,
        "fg3_made": 0, "fg3_attempted": 0,
        "ft_made": 0, "ft_attempted": 0,
        "oreb": 0, "dreb": 0,
        "assists": 0, "steals": 0, "turnovers": 0,
        "blocks": 0, "blocks_against": 0,
        "fouls_drawn": 0, "personal_fouls": 0,
    }

    for e in events:
        et = e["event_type"]

        # 2PT made (close, mid, dunk)
        if et in ("CLOSE_MADE", "MID_MADE", "DUNK_MADE"):
            stats["fg2_made"] += 1
            stats["fg2_attempted"] += 1
            stats["points"] += 2
        # 2PT missed
        elif et in ("CLOSE_MISS", "MID_MISS", "DUNK_MISS"):
            stats["fg2_attempted"] += 1
        # 3PT made
        elif et == "THREE_MADE":
            stats["fg3_made"] += 1
            stats["fg3_attempted"] += 1
            stats["points"] += 3
        # 3PT missed
        elif et == "THREE_MISS":
            stats["fg3_attempted"] += 1
        # Free throws
        elif et == "FT_MADE":
            stats["ft_made"] += 1
            stats["ft_attempted"] += 1
            stats["points"] += 1
        elif et == "FT_MISS":
            stats["ft_attempted"] += 1
        # Other events
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


def _is_starter(
    src: sqlite3.Connection, match_id: str, team: str, player_name: str
) -> bool:
    """Detect if a player is a starter using substitution and event data.

    A player is a starter if they appeared in events or were subbed OUT
    before ever being subbed IN.
    """
    # First sub IN for this player
    first_in = src.execute(
        "SELECT MIN(event_seq) FROM substitutions "
        "WHERE match_id = ? AND team = ? AND player_in = ?",
        (match_id, team, player_name),
    ).fetchone()[0]

    # First sub OUT
    first_out = src.execute(
        "SELECT MIN(event_seq) FROM substitutions "
        "WHERE match_id = ? AND team = ? AND player_out = ?",
        (match_id, team, player_name),
    ).fetchone()[0]

    # First event
    first_event = src.execute(
        "SELECT MIN(event_seq) FROM events "
        "WHERE match_id = ? AND team = ? AND player_name = ?",
        (match_id, team, player_name),
    ).fetchone()[0]

    # Never subbed IN but has events or was subbed out
    if first_in is None and (first_event is not None or first_out is not None):
        return True
    # Subbed OUT before first sub IN
    if first_out is not None and (first_in is None or first_out < first_in):
        return True
    # Event before first sub IN
    if first_event is not None and (first_in is None or first_event < first_in):
        return True

    return False


def _get_player_minutes(
    src: sqlite3.Connection, match_id: str, team: str,
    player_name: str, is_starter: bool
) -> int:
    """Calculate approximate playing time for a single player.

    Uses substitution data + starter detection. Precision: ±1 minute.
    """
    # Get game length from quarter_scores
    row = src.execute(
        "SELECT quarter_scores FROM matches WHERE match_id = ?",
        (match_id,),
    ).fetchone()
    quarters = json.loads(row[0]) if row and row[0] else []
    game_end = len(quarters) * 10 if quarters else 40

    # Get all substitutions involving this player, ordered
    subs = src.execute(
        "SELECT player_in, player_out, minute FROM substitutions "
        "WHERE match_id = ? AND team = ? AND (player_in = ? OR player_out = ?) "
        "ORDER BY event_seq",
        (match_id, team, player_name, player_name),
    ).fetchall()

    total_minutes = 0
    on_court = is_starter
    entered_at = 0 if is_starter else None

    for sub in subs:
        if sub["player_out"] == player_name and on_court:
            # Going out — accumulate time
            total_minutes += sub["minute"] - (entered_at or 0)
            on_court = False
            entered_at = None
        elif sub["player_in"] == player_name and not on_court:
            # Coming in
            on_court = True
            entered_at = sub["minute"]

    # Still on court at game end
    if on_court and entered_at is not None:
        total_minutes += game_end - entered_at

    return total_minutes

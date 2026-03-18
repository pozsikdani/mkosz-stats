from __future__ import annotations
"""Shotchart import: MKOSZ API → unified DB (perzisztált shots tábla).

Portolt logika: mkosz-shotchart/shotchart.py
"""

import re
import sqlite3
import time
import unicodedata

import requests

from config import SHOTCHART_API_URL, SCHEDULE_URL_TPL, API_DELAY


# ── Coordinate normalization (ported from shotchart.py) ──────────

FT_THRESHOLD = 3
BASKET_HX = 50.0
BASKET_HY = 1.575 / 14.0 * 100  # ≈ 11.25%


def _is_free_throw(x: float, y: float) -> bool:
    return (x <= FT_THRESHOLD or x >= 100 - FT_THRESHOLD) and (
        y <= FT_THRESHOLD or y >= 100 - FT_THRESHOLD
    )


def _normalize_to_halfcourt(x: float, y: float, side: str) -> tuple[float, float, bool]:
    """Full-court API coords → half-court (hx, hy, is_ft)."""
    if _is_free_throw(x, y):
        return 50.0, 0.0, True

    if str(side) == "0":
        hx = y
        hy = (100 - x) * 2
    else:
        hx = 100 - y
        hy = x * 2

    hx = max(0.0, min(100.0, hx))
    hy = max(0.0, min(100.0, hy))
    return hx, hy, False


def _classify_shot(hx: float, hy: float, is_ft: bool) -> str:
    """Classify: 'ft', 'paint', 'mid', 'three'."""
    if is_ft:
        return "ft"

    dx_m = (hx - BASKET_HX) / 100.0 * 15.0
    dy_m = (hy - BASKET_HY) / 100.0 * 14.0
    dist_m = (dx_m**2 + dy_m**2) ** 0.5

    if dist_m >= 6.6:
        return "three"

    paint_hx_min = 50 - 2.45 / 15.0 * 100
    paint_hx_max = 50 + 2.45 / 15.0 * 100
    paint_hy_max = 5.8 / 14.0 * 100

    if paint_hx_min <= hx <= paint_hx_max and hy <= paint_hy_max:
        return "paint"

    return "mid"


# ── API functions ────────────────────────────────────────────────

def fetch_shots(gamecode: str, comp: str, season: str) -> list[dict]:
    """Fetch shot chart data from MKOSZ API."""
    resp = requests.post(
        SHOTCHART_API_URL,
        data={
            "f": "getShootchart",
            "gamecode": gamecode,
            "lea": comp,
            "year": season,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        return []
    return data


def fetch_team_schedule(comp: str, season: str, team_id: str) -> list[str]:
    """Fetch all gamecodes for a team from MKOSZ schedule page."""
    url = SCHEDULE_URL_TPL.format(season=season, comp=comp, team_id=team_id)
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()

    pattern = re.compile(rf"{re.escape(comp)}_(\d+)")
    ids = []
    seen = set()
    for m in pattern.finditer(resp.text):
        gc = f"{comp}_{m.group(1)}"
        if gc not in seen:
            seen.add(gc)
            ids.append(gc)
    return ids


# ── Import function ──────────────────────────────────────────────

def import_shotchart(
    conn: sqlite3.Connection,
    all_matches: bool = False,
    comp: str | None = None,
    season: str = "x2526",
    team_id: str | None = None,
):
    """Import shot chart data into unified DB.

    Modes:
    - all_matches=True: fetch for all matches in DB
    - comp + team_id: fetch all matches for a specific team
    """
    if all_matches:
        gamecodes = [
            r[0] for r in conn.execute(
                "SELECT gamecode FROM matches WHERE has_shotchart = 0"
            ).fetchall()
        ]
        print(f"  {len(gamecodes)} meccs shotchart nélkül az adatbázisban")
    elif comp and team_id:
        print(f"  Csapat program lekérése: {team_id} ({comp}, {season})...")
        gamecodes = fetch_team_schedule(comp, season, team_id)
        print(f"  {len(gamecodes)} meccs találva")
    else:
        print("HIBA: all_matches=True vagy comp+team_id szükséges")
        return

    total_shots = 0
    matches_with_data = 0

    for i, gc in enumerate(gamecodes):
        gc_comp, _ = gc.rsplit("_", 1) if "_" in gc else (comp or gc, "")
        gc_season = season

        # Check if already imported
        existing = conn.execute(
            "SELECT COUNT(*) FROM shots WHERE gamecode = ?", (gc,)
        ).fetchone()[0]
        if existing > 0:
            continue

        shots = fetch_shots(gc, gc_comp, gc_season)
        if not shots:
            time.sleep(API_DELAY)
            continue

        # Ensure match exists
        conn.execute(
            """INSERT INTO matches (gamecode, comp_code, season, team_a_name, team_b_name, has_shotchart)
               VALUES (?, ?, ?, '', '', 1)
               ON CONFLICT(gamecode) DO UPDATE SET has_shotchart = 1""",
            (gc, gc_comp, gc_season),
        )

        shot_count = 0
        for s in shots:
            x_raw = float(s["x"])
            y_raw = float(s["y"])
            side = s.get("side", "0")
            hx, hy, is_ft = _normalize_to_halfcourt(x_raw, y_raw, side)
            zone = _classify_shot(hx, hy, is_ft)

            conn.execute(
                """INSERT INTO shots
                   (gamecode, playercode, player_name, team_id, period,
                    x_raw, y_raw, side, hx, hy,
                    is_made, is_free_throw, zone)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    gc,
                    s.get("playercode"),
                    s.get("wbname") or s.get("firstname", ""),
                    int(s["team_id"]) if s.get("team_id") else None,
                    int(s.get("period", 0)),
                    x_raw, y_raw, side, hx, hy,
                    1 if s.get("is_successfull") else 0,
                    1 if is_ft else 0,
                    zone,
                ),
            )
            shot_count += 1

        total_shots += shot_count
        matches_with_data += 1
        print(f"  {gc}: {shot_count} dobás")

        if (i + 1) % 10 == 0:
            conn.commit()

        time.sleep(API_DELAY)

    conn.commit()
    print(f"\n🎯 Shotchart import kész: {matches_with_data} meccs, {total_shots} dobás")

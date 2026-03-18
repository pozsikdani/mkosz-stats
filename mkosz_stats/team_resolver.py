from __future__ import annotations
"""Csapatnév → team_id feloldás és alias kezelés."""

import sqlite3
from .normalize import normalize_name


def resolve_team(conn: sqlite3.Connection, team_name: str) -> int | None:
    """Csapatnév → team_id feloldás az alias tábla alapján."""
    if not team_name:
        return None

    # Exact alias match
    row = conn.execute(
        "SELECT team_id FROM team_aliases WHERE alias = ?", (team_name,)
    ).fetchone()
    if row:
        return row[0]

    # Normalized match
    norm = normalize_name(team_name)
    rows = conn.execute(
        "SELECT alias, team_id FROM team_aliases"
    ).fetchall()
    for r in rows:
        if normalize_name(r["alias"]) == norm:
            return r["team_id"]

    return None


def register_team(
    conn: sqlite3.Connection,
    team_id: int,
    short_name: str,
    full_name: str | None = None,
) -> None:
    """Csapat regisztrálás (upsert)."""
    conn.execute(
        """INSERT INTO teams (team_id, short_name, full_name)
           VALUES (?, ?, ?)
           ON CONFLICT(team_id) DO UPDATE SET
             short_name = COALESCE(excluded.short_name, teams.short_name),
             full_name = COALESCE(excluded.full_name, teams.full_name)""",
        (team_id, short_name, full_name),
    )


def register_alias(
    conn: sqlite3.Connection, alias: str, team_id: int
) -> None:
    """Csapatnév alias regisztrálás."""
    if not alias:
        return
    conn.execute(
        "INSERT OR IGNORE INTO team_aliases (alias, team_id) VALUES (?, ?)",
        (alias, team_id),
    )


def ensure_season(conn: sqlite3.Connection, season: str) -> None:
    """Season sor biztosítás."""
    if not season:
        return
    # x2526 → 2025/2026
    digits = season.lstrip("x")
    if len(digits) == 4:
        label = f"20{digits[:2]}/20{digits[2:]}"
    else:
        label = season
    conn.execute(
        "INSERT OR IGNORE INTO seasons (season_code, label) VALUES (?, ?)",
        (season, label),
    )


def ensure_competition(
    conn: sqlite3.Connection,
    comp_code: str,
    season: str,
    comp_name: str | None = None,
    level: str | None = None,
    gender: str = "M",
) -> None:
    """Competition sor biztosítás."""
    ensure_season(conn, season)
    conn.execute(
        """INSERT INTO competitions (comp_code, season, comp_name, level, gender)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(comp_code, season) DO UPDATE SET
             comp_name = COALESCE(excluded.comp_name, competitions.comp_name),
             level = COALESCE(excluded.level, competitions.level),
             gender = COALESCE(excluded.gender, competitions.gender)""",
        (comp_code, season, comp_name, level, gender),
    )

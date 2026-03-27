"""Adatbázis séma létrehozás és connection management."""

import sqlite3
from pathlib import Path

SCHEMA_VERSION = 1

SCHEMA_SQL = """
-- ============================================================
-- LOOKUP / DIMENSION TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS seasons (
    season_code  TEXT PRIMARY KEY,
    label        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS competitions (
    comp_code    TEXT NOT NULL,
    season       TEXT NOT NULL REFERENCES seasons(season_code),
    comp_name    TEXT,
    level        TEXT,
    gender       TEXT DEFAULT 'M',
    PRIMARY KEY (comp_code, season)
);

CREATE TABLE IF NOT EXISTS teams (
    team_id      INTEGER PRIMARY KEY,
    short_name   TEXT,
    full_name    TEXT
);

CREATE TABLE IF NOT EXISTS team_aliases (
    alias        TEXT PRIMARY KEY,
    team_id      INTEGER REFERENCES teams(team_id)
);

CREATE TABLE IF NOT EXISTS players (
    playercode       TEXT PRIMARY KEY,
    license_number   TEXT UNIQUE,
    canonical_name   TEXT NOT NULL,
    birth_year       INTEGER,
    height_cm        INTEGER,
    position         TEXT
);

CREATE TABLE IF NOT EXISTS player_names (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    playercode   TEXT REFERENCES players(playercode),
    name_variant TEXT NOT NULL,
    source       TEXT NOT NULL,
    UNIQUE(playercode, name_variant, source)
);

CREATE TABLE IF NOT EXISTS rosters (
    playercode   TEXT NOT NULL REFERENCES players(playercode),
    team_id      INTEGER NOT NULL REFERENCES teams(team_id),
    comp_code    TEXT NOT NULL,
    season       TEXT NOT NULL,
    jersey_number INTEGER,
    FOREIGN KEY (comp_code, season) REFERENCES competitions(comp_code, season),
    PRIMARY KEY (playercode, team_id, comp_code, season)
);

-- ============================================================
-- MATCH / FACT TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS matches (
    gamecode             TEXT PRIMARY KEY,
    comp_code            TEXT NOT NULL,
    season               TEXT NOT NULL,
    round_name           TEXT,
    match_date           TEXT,
    match_time           TEXT,
    venue                TEXT,
    team_a_id            INTEGER REFERENCES teams(team_id),
    team_b_id            INTEGER REFERENCES teams(team_id),
    team_a_name          TEXT NOT NULL,
    team_b_name          TEXT NOT NULL,
    score_a              INTEGER,
    score_b              INTEGER,
    quarter_scores       TEXT,
    referees             TEXT,
    has_scoresheet       INTEGER DEFAULT 0,
    has_pbp              INTEGER DEFAULT 0,
    has_shotchart        INTEGER DEFAULT 0,
    scoresheet_match_id  TEXT
);

CREATE TABLE IF NOT EXISTS player_game_stats (
    gamecode         TEXT NOT NULL REFERENCES matches(gamecode),
    playercode       TEXT REFERENCES players(playercode),
    team             TEXT NOT NULL CHECK (team IN ('A','B')),
    player_name      TEXT NOT NULL,
    license_number   TEXT,
    jersey_number    INTEGER,
    is_starter       INTEGER DEFAULT 0,
    entry_quarter    INTEGER,
    -- Basic stats (scoresheet)
    points           INTEGER DEFAULT 0,
    fg2_made         INTEGER,
    fg2_attempted    INTEGER,
    fg3_made         INTEGER,
    fg3_attempted    INTEGER,
    ft_made          INTEGER,
    ft_attempted     INTEGER,
    personal_fouls   INTEGER,
    -- Advanced stats (PBP)
    minutes          INTEGER,
    oreb             INTEGER,
    dreb             INTEGER,
    assists          INTEGER,
    steals           INTEGER,
    turnovers        INTEGER,
    blocks           INTEGER,
    blocks_against   INTEGER,
    fouls_drawn      INTEGER,
    plus_minus       INTEGER,
    -- Computed
    val              INTEGER,
    ts_pct           REAL,
    efg_pct          REAL,
    game_score       REAL,
    usg_pct          REAL,
    ast_to           REAL,
    tov_pct          REAL,
    -- Meta
    source           TEXT NOT NULL,
    PRIMARY KEY (gamecode, team, player_name)
);

CREATE TABLE IF NOT EXISTS scoring_events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    gamecode         TEXT NOT NULL REFERENCES matches(gamecode),
    event_seq        INTEGER NOT NULL,
    quarter          INTEGER NOT NULL,
    minute           TEXT,
    team             TEXT NOT NULL CHECK (team IN ('A','B')),
    playercode       TEXT REFERENCES players(playercode),
    license_number   TEXT,
    jersey_number    INTEGER,
    points           INTEGER NOT NULL,
    shot_type        TEXT CHECK (shot_type IN ('2FG','3FG','FT','MULTI')),
    made             INTEGER NOT NULL CHECK (made IN (0,1)),
    score_a          INTEGER NOT NULL,
    score_b          INTEGER NOT NULL,
    UNIQUE(gamecode, event_seq)
);

CREATE TABLE IF NOT EXISTS pbp_events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    gamecode         TEXT NOT NULL REFERENCES matches(gamecode),
    event_seq        INTEGER NOT NULL,
    quarter          INTEGER NOT NULL,
    minute           INTEGER,
    team             TEXT NOT NULL CHECK (team IN ('A','B')),
    playercode       TEXT REFERENCES players(playercode),
    player_name      TEXT,
    event_type       TEXT NOT NULL,
    event_raw        TEXT,
    counter          INTEGER,
    score_a          INTEGER,
    score_b          INTEGER,
    is_scoring       INTEGER DEFAULT 0,
    points           INTEGER DEFAULT 0,
    UNIQUE(gamecode, event_seq)
);

CREATE TABLE IF NOT EXISTS substitutions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    gamecode         TEXT NOT NULL REFERENCES matches(gamecode),
    event_seq        INTEGER NOT NULL,
    quarter          INTEGER NOT NULL,
    minute           INTEGER,
    team             TEXT NOT NULL CHECK (team IN ('A','B')),
    player_in_code   TEXT REFERENCES players(playercode),
    player_out_code  TEXT REFERENCES players(playercode),
    player_in_name   TEXT NOT NULL,
    player_out_name  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS timeouts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    gamecode         TEXT NOT NULL REFERENCES matches(gamecode),
    event_seq        INTEGER,
    quarter          INTEGER NOT NULL,
    minute           TEXT,
    team             TEXT NOT NULL CHECK (team IN ('A','B')),
    source           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS personal_fouls (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    gamecode         TEXT NOT NULL REFERENCES matches(gamecode),
    team             TEXT NOT NULL CHECK (team IN ('A','B')),
    playercode       TEXT REFERENCES players(playercode),
    jersey_number    INTEGER,
    foul_number      INTEGER NOT NULL,
    minute           TEXT,
    quarter          INTEGER NOT NULL,
    foul_type        TEXT DEFAULT 'defensive',
    foul_category    TEXT,
    free_throws      INTEGER,
    offsetting       INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS shots (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    gamecode         TEXT NOT NULL REFERENCES matches(gamecode),
    playercode       TEXT REFERENCES players(playercode),
    player_name      TEXT,
    team_id          INTEGER,
    team             TEXT CHECK (team IN ('A','B')),
    period           INTEGER NOT NULL,
    x_raw            REAL NOT NULL,
    y_raw            REAL NOT NULL,
    side             TEXT,
    hx               REAL,
    hy               REAL,
    is_made          INTEGER NOT NULL,
    is_free_throw    INTEGER DEFAULT 0,
    zone             TEXT,
    fetched_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS quarter_scores (
    gamecode     TEXT NOT NULL REFERENCES matches(gamecode),
    quarter      TEXT NOT NULL,
    score_a      INTEGER,
    score_b      INTEGER,
    PRIMARY KEY (gamecode, quarter)
);

-- ============================================================
-- WEB-SCRAPED DATA
-- ============================================================

CREATE TABLE IF NOT EXISTS standings (
    comp_code        TEXT NOT NULL,
    season           TEXT NOT NULL,
    team_name        TEXT NOT NULL,
    rank             INTEGER,
    games_played     INTEGER,
    wins             INTEGER,
    losses           INTEGER,
    home_record      TEXT,
    away_record      TEXT,
    streak           TEXT,
    last_five        TEXT,
    team_page_url    TEXT,
    team_id          INTEGER,
    scraped_at       TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (comp_code, season, team_name)
);

CREATE TABLE IF NOT EXISTS player_roster_meta (
    comp_code        TEXT NOT NULL,
    season           TEXT NOT NULL,
    team_name        TEXT NOT NULL,
    player_name      TEXT NOT NULL,
    jersey_number    TEXT,
    position         TEXT,
    height_cm        INTEGER,
    photo_url        TEXT,
    birth_year       TEXT,
    scraped_at       TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (comp_code, season, team_name, player_name)
);

CREATE TABLE IF NOT EXISTS web_match_results (
    comp_code        TEXT NOT NULL,
    season           TEXT NOT NULL,
    match_date       TEXT,
    home_team        TEXT NOT NULL,
    away_team        TEXT NOT NULL,
    home_score       INTEGER,
    away_score       INTEGER,
    team_id          INTEGER,
    scraped_at       TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (comp_code, season, match_date, home_team, away_team)
);

-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_matches_comp ON matches(comp_code, season);
CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date);
CREATE INDEX IF NOT EXISTS idx_pgs_gamecode ON player_game_stats(gamecode);
CREATE INDEX IF NOT EXISTS idx_pgs_playercode ON player_game_stats(playercode);
CREATE INDEX IF NOT EXISTS idx_se_gamecode ON scoring_events(gamecode);
CREATE INDEX IF NOT EXISTS idx_pbp_gamecode ON pbp_events(gamecode);
CREATE INDEX IF NOT EXISTS idx_pbp_playercode ON pbp_events(playercode);
CREATE INDEX IF NOT EXISTS idx_shots_gamecode ON shots(gamecode);
CREATE INDEX IF NOT EXISTS idx_shots_playercode ON shots(playercode);
CREATE INDEX IF NOT EXISTS idx_pf_gamecode ON personal_fouls(gamecode);
CREATE INDEX IF NOT EXISTS idx_subs_gamecode ON substitutions(gamecode);
CREATE INDEX IF NOT EXISTS idx_player_names_variant ON player_names(name_variant);
CREATE INDEX IF NOT EXISTS idx_team_aliases_team ON team_aliases(team_id);
CREATE INDEX IF NOT EXISTS idx_rosters_team ON rosters(team_id, season);

-- ============================================================
-- META
-- ============================================================

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


def get_connection(db_path: str, foreign_keys: bool = False) -> sqlite3.Connection:
    """SQLite connection megnyitás WAL mode-ban.

    foreign_keys=False by default — az import fázisban a playercode FK-k
    még nem teljesülnek (a players tábla a resolve fázisban töltődik).
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    if foreign_keys:
        conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str) -> sqlite3.Connection:
    """Adatbázis inicializálás: séma létrehozás."""
    conn = get_connection(db_path)
    conn.executescript(SCHEMA_SQL)
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?)",
        (str(SCHEMA_VERSION),),
    )
    conn.commit()
    print(f"Adatbázis inicializálva: {db_path}")
    return conn


def db_status(conn: sqlite3.Connection) -> dict:
    """DB statisztikák lekérdezése."""
    tables = [
        "seasons", "competitions", "teams", "players", "matches",
        "player_game_stats", "shots", "pbp_events", "scoring_events",
        "substitutions", "timeouts", "personal_fouls", "rosters",
        "player_names", "team_aliases", "quarter_scores",
        "standings", "player_roster_meta", "web_match_results",
    ]
    counts = {}
    for t in tables:
        try:
            row = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
            counts[t] = row[0]
        except Exception:
            counts[t] = 0
    return counts

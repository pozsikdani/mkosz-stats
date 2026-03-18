"""Adatmodell dataclass-ok — köztes reprezentáció import során."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Player:
    playercode: Optional[str] = None
    license_number: Optional[str] = None
    canonical_name: str = ""
    birth_year: Optional[int] = None
    height_cm: Optional[int] = None
    position: Optional[str] = None


@dataclass
class Match:
    gamecode: str = ""
    comp_code: str = ""
    season: str = ""
    match_date: Optional[str] = None
    match_time: Optional[str] = None
    venue: Optional[str] = None
    team_a_name: str = ""
    team_b_name: str = ""
    team_a_id: Optional[int] = None
    team_b_id: Optional[int] = None
    score_a: Optional[int] = None
    score_b: Optional[int] = None
    quarter_scores: Optional[str] = None
    has_scoresheet: int = 0
    has_pbp: int = 0
    has_shotchart: int = 0
    scoresheet_match_id: Optional[str] = None


@dataclass
class PlayerGameStats:
    gamecode: str = ""
    playercode: Optional[str] = None
    team: str = ""  # 'A' or 'B'
    player_name: str = ""
    jersey_number: Optional[int] = None
    is_starter: int = 0
    points: int = 0
    fg2_made: Optional[int] = None
    fg2_attempted: Optional[int] = None
    fg3_made: Optional[int] = None
    fg3_attempted: Optional[int] = None
    ft_made: Optional[int] = None
    ft_attempted: Optional[int] = None
    personal_fouls: Optional[int] = None
    minutes: Optional[int] = None
    oreb: Optional[int] = None
    dreb: Optional[int] = None
    assists: Optional[int] = None
    steals: Optional[int] = None
    turnovers: Optional[int] = None
    blocks: Optional[int] = None
    plus_minus: Optional[int] = None
    val: Optional[int] = None
    ts_pct: Optional[float] = None
    efg_pct: Optional[float] = None
    game_score: Optional[float] = None
    usg_pct: Optional[float] = None
    ast_to: Optional[float] = None
    tov_pct: Optional[float] = None
    source: str = ""


@dataclass
class Shot:
    gamecode: str = ""
    playercode: Optional[str] = None
    player_name: str = ""
    team_id: Optional[int] = None
    period: int = 0
    x_raw: float = 0.0
    y_raw: float = 0.0
    side: str = ""
    hx: float = 0.0
    hy: float = 0.0
    is_made: int = 0
    is_free_throw: int = 0
    zone: str = ""

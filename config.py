"""Konfiguráció: forrás DB útvonalak, API URL-ek, konstansok."""

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Unified database
DB_PATH = os.path.join(BASE_DIR, "mkosz_stats.sqlite")

# Source databases — sibling repos under the same parent directory
PARENT_DIR = os.path.dirname(BASE_DIR)
SCORESHEET_DB = os.path.join(PARENT_DIR, "mkosz-scoresheet", "scoresheet.sqlite")
PBP_DB = os.path.join(PARENT_DIR, "mkosz-play-by-play", "pbp.sqlite")

# MKOSZ API
SHOTCHART_API_URL = "https://mkosz.hu/ajax/film.php"
SCHEDULE_URL_TPL = (
    "https://mkosz.hu/bajnoksag-musor/{season}/{comp}/phase/0/csapat/{team_id}"
)
PLAYER_PAGE_URL_TPL = (
    "https://mkosz.hu/jatekos/{season}/{comp}/{playercode}/{slug}"
)

# Web scraping URLs
STANDINGS_URL_TPL = "https://mkosz.hu/bajnoksag/{season}/{comp}"
ROSTER_URL_TPL = "https://mkosz.hu/csapat/{season}/{comp}/0/{slug}"

# Rate limiting
API_DELAY = 0.25  # seconds between API calls

# Known competition metadata
COMPETITIONS = {
    # comp_code: (comp_name, level, gender)
    "hun2a": ("NB1 B Piros", "nb1b", "M"),
    "hun2b": ("NB1 B Zöld", "nb1b", "M"),
    "hun3k": ("NB2 Kelet", "nb2", "M"),
    "hun3kob": ("NB2 Közép B", "nb2", "M"),
    "hun3koa": ("NB2 Közép A", "nb2", "M"),
    "hun3ki": ("NB2 Kiemelt", "nb2", "M"),
    "hun3n": ("NB2 Nyugat", "nb2", "M"),
    "hun3_plya": ("NB2 Alsóházi Rájátszás", "nb2_playoff", "M"),
    "whun": ("Női NB1 A", "nb1", "F"),
    "whun_univn": ("MEFOB Női", "university", "F"),
    "hun_univn": ("MEFOB Férfi", "university", "M"),
    "whun_bud_na": ("Budapest Női A", "regional", "F"),
    "hun_bud_rkfb": ("Budapest Regionális", "regional", "M"),
}

# Scoresheet match_id prefix → comp_code mapping
SCORESHEET_PREFIX_TO_COMP = {
    "F2KI": "hun3ki",
    "F2KA": "hun3koa",
    "F2KB": "hun3kob",
    "F2KE": "hun3k",
    "F2NY": "hun3n",
    "NA": "whun_bud_na",
    "RKFB": "hun_bud_rkfb",
    # NB1B
    "F1PF": "hun2a",
    "F1ZF": "hun2b",
    # MEFOB
    "FEBN": "hun_univn",
    "NEBN": "whun_univn",
}

# Melyik bajnoksághoz milyen adatforrás elérhető
COMP_DATA_SOURCES = {
    # NB2 + Budapest: scoresheet only
    "hun3k": ["scoresheet"],
    "hun3kob": ["scoresheet"],
    "hun3koa": ["scoresheet"],
    "hun3ki": ["scoresheet"],
    "hun3n": ["scoresheet"],
    "hun3_plya": ["scoresheet"],
    "whun_bud_na": ["scoresheet"],
    "hun_bud_rkfb": ["scoresheet"],
    # NB1B: PBP + shotchart (nincs scoresheet)
    "hun2a": ["pbp", "shotchart"],
    "hun2b": ["pbp", "shotchart"],
    # MEFOB: PBP only
    "whun_univn": ["pbp"],
    "hun_univn": ["pbp"],
    # Női NB1: shotchart only (egyelőre)
    "whun": ["shotchart"],
}

"""Név normalizálás és fuzzy match segédfüggvények.

Portolva: mkosz-shotchart/shotchart.py normalize_name() + match_player()
"""

from __future__ import annotations

import unicodedata
import re
from typing import Optional


def normalize_name(name: str) -> str:
    """Kisbetűs, ékezet nélküli, title-stripped név.

    Pl. 'Hegedűs Brúnó' → 'hegedus bruno'
        'DR. SÁROSI MIKLÓS' → 'sarosi miklos'
    """
    if not name:
        return ""
    s = name.lower().strip()
    # Remove common prefixes
    s = re.sub(r"^(dr\.?\s*|ifj\.?\s*|id\.?\s*)", "", s)
    # Strip accents: NFKD decomposition → remove combining chars
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def names_match(a: str, b: str) -> bool:
    """Fuzzy név egyezés: normalizált substring match mindkét irányban."""
    na, nb = normalize_name(a), normalize_name(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    # Substring match either way
    return na in nb or nb in na


def name_similarity(a: str, b: str) -> float:
    """0-1 hasonlósági score két név között.

    1.0 = exact match (normalizálva)
    0.8+ = substring match
    0.0-0.8 = token overlap alapú
    """
    na, nb = normalize_name(a), normalize_name(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    if na in nb or nb in na:
        return 0.9

    # Token overlap (Jaccard)
    tokens_a = set(na.split())
    tokens_b = set(nb.split())
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)


def extract_gamecode_from_pdf(source_pdf: str) -> Optional[str]:
    """Scoresheet source_pdf → gamecode.

    'hun3k_125483.pdf' → 'hun3k_125483'
    'whun_bud_na_92.pdf' → 'whun_bud_na_92'
    """
    if not source_pdf:
        return None
    return source_pdf.replace(".pdf", "").strip()


def split_gamecode(gamecode: str) -> "tuple[str, str]":
    """Gamecode → (comp_code, game_id).

    'hun3k_125483' → ('hun3k', '125483')
    'whun_bud_na_92' → ('whun_bud_na', '92')

    Logika: az utolsó _ utáni rész a game_id (mindig szám).
    """
    parts = gamecode.rsplit("_", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0], parts[1]
    # Fallback: próbáljunk ismert prefix-eket
    return gamecode, ""

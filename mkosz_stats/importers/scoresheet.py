"""Scoresheet import: scoresheet.sqlite → unified DB."""

import sqlite3
from ..normalize import extract_gamecode_from_pdf, season_for_date, split_gamecode
from ..team_resolver import ensure_competition, ensure_season
from config import SCORESHEET_PREFIX_TO_COMP, COMPETITIONS


def _takarit_elavult_nevsorok(conn: sqlite3.Connection, beerkezett: dict) -> int:
    """Kiveszi azokat a player_game_stats sorokat, amiket egy átnevezés hagyott ott.

    Miért kell: a jegyzőkönyv-import upsertjének kulcsa (gamecode, team,
    player_name). Ha a jegyzőkönyv-olvasó javítása megváltoztatja egy játékos
    kiolvasott nevét, az ÚJ név ÚJ sort kap, a régi pedig ott marad, és onnantól
    minden összesítés kétszer számolja. (Mérve 2026-11-20: 377 ilyen
    licenc-csoport 93 meccsen; pl. „ALFARAG OSAMA FARAJ" az „ALFARAG OSAMA
    FARAJ HASHEM" mellett — a box score 68 helyett 74 pontot adott.)

    Teljes DELETE nem járható, mint a personal_fouls-nál: a 'merged' sorokon
    PBP-ből jövő adat is van (assziszt, perc, +/-), amit a jegyzőkönyv nem tud
    visszaadni. Ezért kizárólag a LICENC alapján takarítunk: az azonos licenc
    bizonyítja, hogy ugyanarról a játékosról van szó.

    Két eset van, és külön kell kezelni őket:

    1. A mostani kiolvasás NEVESÍTVE adja a licencet → minden más nevű sor
       ugyanazzal a licenccel elavult, mehet.
    2. A mostani kiolvasás ÜRES névvel adja (a kiolvasónak van egy hibája, ami
       35 helyen nem olvassa ki a nevet) → ilyenkor a névtelen sort dobjuk el,
       nem a nevesítettet, mert különben a játékos neve ÉS a PBP-adata is
       elveszik, és csak egy névtelen sor marad a meccslapon.

    Plauzibilitás-gárd: ha a beérkezett névsor pontösszege nem éri el a
    hivatalos eredményt, a kiolvasás csonka (mérve: huna_cup_1058 A csapat 62
    helyett 15 pontot adott), és a takarítás élő sort vinne el. Ilyenkor
    inkább nem takarítunk.

    A futások STABILAK, de nem nulla-törlésűek: a 2. eset minden futáson kivesz
    ~24 sort, mert a forrásban benne vannak az üres nevű sorok, az import
    beszúrja őket, a takarítás pedig ugyanabban a futásban visszaveszi. A
    sorhalmaz ettől nem mozog (mérve: 2. futás 0 eltűnt / 0 új sor).

    MÉRÉS: a CI sorrendjével (jegyzőkönyv → PBP) kell mérni. Csak a
    jegyzőkönyv-importot futtatva a `points = excluded.points` miatt 12
    csapat-meccs rosszabbnak látszik, amit a rá következő PBP-import visszaír.
    Teljes ciklussal: 2564 → 2638 helyes csapat-meccs 2786-ból, 74 javult,
    0 romlott, 0 elveszett PBP-adat.

    AMI BENNE MARAD (külön javítás kell hozzá):
    - 74 duplikált licenc-csoport olyan meccseken, amiknek nincs jegyzőkönyve
      (régi, .upper() nélküli PBP-importból: „CSABA GYÖRGY" + „Csaba György").
    - 14 üres nevű sor, aminek nincs nevesített párja — a kiolvasó nem olvasta
      ki a nevet (35 ilyen sor a forrásban).
    - Két üres nevű sor EGY csapaton belül a (gamecode, team, player_name)
      kulcson ütközik, és az egyik felülírja a másikat (pl. huna_cup_1059 B).
    - A gyökér az, hogy a kulcsban a NÉV van: ha (gamecode, team,
      jersey_number) vagy a licenc lenne a kulcs, az átnevezés helyben
      frissülne, és semmilyen takarítás nem kellene.
    """
    elavult = 0
    for (gamecode, team), b in beerkezett.items():
        hivatalos = conn.execute(
            "SELECT score_a, score_b FROM matches WHERE gamecode = ?", (gamecode,)
        ).fetchone()
        if hivatalos:
            var = hivatalos[0] if team == "A" else hivatalos[1]
            if var and b["pont"] < var:
                continue

        # 1. eset: neves licenc → a más nevű sorok elavultak.
        if b["lic_nevvel"]:
            lic_ph = ",".join("?" * len(b["lic_nevvel"]))
            nev_ph = ",".join("?" * len(b["nevek"]))
            cur = conn.execute(
                f"""DELETE FROM player_game_stats
                     WHERE gamecode = ? AND team = ?
                       AND license_number IN ({lic_ph})
                       AND player_name NOT IN ({nev_ph})""",
                (gamecode, team, *b["lic_nevvel"], *b["nevek"]),
            )
            elavult += cur.rowcount

        # 2. eset: név nélkül jött a licenc → a névtelen sort dobjuk, ha van
        # ugyanarra a licencre nevesített sor is.
        if b["lic_nev_nelkul"]:
            lic_ph = ",".join("?" * len(b["lic_nev_nelkul"]))
            cur = conn.execute(
                f"""DELETE FROM player_game_stats
                     WHERE gamecode = ? AND team = ?
                       AND license_number IN ({lic_ph})
                       AND TRIM(COALESCE(player_name, '')) = ''
                       AND EXISTS (
                             SELECT 1 FROM player_game_stats p2
                              WHERE p2.gamecode = player_game_stats.gamecode
                                AND p2.team = player_game_stats.team
                                AND p2.license_number = player_game_stats.license_number
                                AND TRIM(COALESCE(p2.player_name, '')) <> '')""",
                (gamecode, team, *b["lic_nev_nelkul"]),
            )
            elavult += cur.rowcount
    return elavult


def import_scoresheet(conn: sqlite3.Connection, src_path: str, season: str = None):
    """Import all data from scoresheet SQLite database.

    A szezon meccsenként a dátumából jön (season_for_date); a `season` csak
    tartalék, ha egy meccshez nincs dátum. Így egy szezonváltás nem igényel
    kódmódosítást, és egy régi PDF utólagos betöltése sem csúszik a mai
    szezonba.
    """
    src = sqlite3.connect(src_path)
    src.row_factory = sqlite3.Row

    seasons_seen = set()

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

        match_season = season_for_date(m["match_date"]) or season
        if not match_season:
            print(f"  ⚠️  Nincs dátum és nincs --season: {gamecode} kihagyva")
            continue
        if match_season not in seasons_seen:
            ensure_season(conn, match_season)
            seasons_seen.add(match_season)

        # Ensure competition exists
        comp_info = COMPETITIONS.get(comp_code)
        if comp_info:
            ensure_competition(conn, comp_code, match_season, *comp_info)
        else:
            ensure_competition(conn, comp_code, match_season)

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
                 season = excluded.season,
                 scoresheet_match_id = excluded.scoresheet_match_id""",
            (
                gamecode, comp_code, match_season,
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

    # (gamecode, team) → mit adott a mostani kiolvasás. Ebből takarítjuk ki az
    # átnevezés miatt ottmaradt sorokat, lásd a ciklus utáni blokkot.
    beerkezett = {}

    stats_count = 0
    for s in stats:
        gamecode = gamecode_map.get(s["match_id"])
        if not gamecode:
            continue

        b = beerkezett.setdefault((gamecode, s["team"]),
                                  {"nevek": set(), "lic_nevvel": set(),
                                   "lic_nev_nelkul": set(), "pont": 0})
        nev = (s["name"] or "").strip()
        b["pont"] += s["points"] or 0
        if nev:
            b["nevek"].add(s["name"])
            if s["license_number"]:
                b["lic_nevvel"].add(s["license_number"])
        elif s["license_number"]:
            b["lic_nev_nelkul"].add(s["license_number"])

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
                 personal_fouls = excluded.personal_fouls,
                 -- PBP-sorra írt jegyzőkönyvi adat → 'merged', különben a PBP-import
                 -- meccsenkénti takarítása (DELETE source='pbp') elvinné.
                 source = CASE WHEN player_game_stats.source = 'pbp' THEN 'merged'
                               ELSE player_game_stats.source END""",
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
    elavult = _takarit_elavult_nevsorok(conn, beerkezett)
    if elavult:
        print(f"  🧹 {elavult} elavult (átnevezett) player_game_stats sor törölve")

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
    # Wholesale re-import (source scoresheet DB is source of truth) — without
    # DELETE the plain INSERT below appended every day.
    conn.execute("DELETE FROM timeouts WHERE source='scoresheet'")
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
    # Wholesale re-import — without DELETE the plain INSERT below appended
    # every day → ~20× row duplication (24 MB hely-pazarlás).
    conn.execute("DELETE FROM personal_fouls")
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

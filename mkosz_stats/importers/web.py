"""Web scraper: standings, roster metadata, match results from mkosz.hu."""

import re
import sqlite3
import time
import unicodedata

import requests
from bs4 import BeautifulSoup

from config import API_DELAY, COMPETITIONS

BASE_URL = "https://mkosz.hu"
USER_AGENT = "Mozilla/5.0 mkosz-stats"

HU_MONTHS = {
    "január": 1, "február": 2, "március": 3, "április": 4,
    "május": 5, "június": 6, "július": 7, "augusztus": 8,
    "szeptember": 9, "október": 10, "november": 11, "december": 12,
}


def _parse_hu_date(s):
    """Parse '2025. október 7.' -> '2025-10-07'."""
    m = re.match(r"(\d{4})\.\s*(\S+)\s+(\d{1,2})\.?", s.strip())
    if not m:
        return None
    year, month_str, day = m.group(1), m.group(2).rstrip("."), m.group(3)
    month = HU_MONTHS.get(month_str.lower())
    if not month:
        return None
    return f"{year}-{month:02d}-{int(day):02d}"


def _fetch(url):
    """GET with timeout and user-agent."""
    resp = requests.get(url, timeout=15, headers={"User-Agent": USER_AGENT})
    resp.encoding = "utf-8"
    return resp


def scrape_standings(season, comp):
    """Scrape league standings from mkosz.hu/bajnoksag/{season}/{comp}.

    Returns list of dicts with: team_name, rank, gp, wins, losses,
    streak, home_record, away_record, last_five, team_page_url.
    """
    url = f"{BASE_URL}/bajnoksag/{season}/{comp}"
    resp = _fetch(url)
    soup = BeautifulSoup(resp.content.decode("utf-8", errors="replace"), "html.parser")

    standings = []
    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if len(rows) < 10:
            continue
        header_cells = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
        if not any("Csapat" in h or "%" in h for h in header_cells):
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            tds = row.find_all("td")
            if len(cells) < 10:
                continue

            rank = cells[0].rstrip(".")
            team_name = cells[2]

            # Extract team page URL
            team_link = tds[2].find("a") if len(tds) > 2 else None
            href = team_link["href"] if team_link and team_link.get("href") else None
            if href and href.startswith("/"):
                team_page_url = BASE_URL + href
            elif href and href.startswith("http"):
                team_page_url = href
            else:
                team_page_url = None

            # Extract team_id from URL
            team_id = None
            if team_page_url:
                tid_match = re.search(r"/(\d{4,5})/", team_page_url)
                if tid_match:
                    team_id = int(tid_match.group(1))

            gp = int(cells[3]) if cells[3].isdigit() else 0
            wins = int(cells[6]) if cells[6].isdigit() else 0
            losses = int(cells[7]) if cells[7].isdigit() else 0
            streak_raw = cells[10] if len(cells) > 10 else ""
            streak = streak_raw.replace("GY", "W").replace("V", "L")
            home_rec = cells[11] if len(cells) > 11 else ""
            away_rec = cells[12] if len(cells) > 12 else ""
            last5 = cells[13] if len(cells) > 13 else ""

            standings.append({
                "team_name": team_name,
                "rank": int(rank) if rank.isdigit() else None,
                "gp": gp,
                "wins": wins,
                "losses": losses,
                "streak": streak,
                "home_record": home_rec,
                "away_record": away_rec,
                "last_five": last5,
                "team_page_url": team_page_url,
                "team_id": team_id,
            })
        break  # only first matching table

    return standings


def scrape_roster(team_page_url):
    """Scrape player roster from a team page URL.

    Returns list of dicts with: player_name, jersey_number, position,
    height_cm, photo_url, birth_year.
    """
    resp = _fetch(team_page_url)
    soup = BeautifulSoup(resp.text, "html.parser")

    players = []
    for row in soup.select("table tr")[1:]:
        cols = row.find_all("td")
        if len(cols) < 5:
            continue

        jersey = cols[0].get_text(strip=True)
        link = cols[1].find("a")
        name = link.get("title", "").strip() if link else cols[1].get_text(strip=True)
        birth = cols[2].get_text(strip=True)
        pos = cols[3].get_text(strip=True)
        height_str = cols[4].get_text(strip=True).replace(" cm", "").replace("cm", "")

        # Extract photo URL from style
        pic_div = cols[1].find("div", class_="team-players-pic")
        pic_style = pic_div.get("style", "") if pic_div else ""
        pic_match = re.search(r"url\(([^)]+)\)", pic_style)
        photo_url = pic_match.group(1) if pic_match else ""
        if "placeholder" in photo_url:
            photo_url = ""

        if name:
            height_cm = int(height_str) if height_str.isdigit() else None
            players.append({
                "player_name": name,
                "jersey_number": jersey,
                "position": pos,
                "height_cm": height_cm,
                "photo_url": photo_url,
                "birth_year": birth,
            })

    return players


def scrape_match_results(season, comp, team_id):
    """Scrape match results from a team's schedule page.

    Returns list of dicts with: match_date, home_team, away_team,
    home_score, away_score.
    """
    url = f"{BASE_URL}/bajnoksag-musor/{season}/{comp}/phase/0/csapat/{team_id}"
    resp = _fetch(url)
    html = resp.content.decode("utf-8", errors="replace")

    results = []
    trs = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL)
    for tr in trs:
        tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.DOTALL)
        if len(tds) != 6:
            continue

        teams = re.findall(r'title="([^"]+)"', tds[0] + tds[1])
        if len(teams) != 2:
            continue
        home_team, away_team = teams[0], teams[1]

        date_m = re.search(r"<b>(.*?)</b>", tds[2])
        if not date_m:
            continue
        date_str = _parse_hu_date(date_m.group(1))
        if not date_str:
            continue

        score_m = re.search(r"(\d+)\s*-\s*(\d+)", tds[4])
        if score_m:
            home_score = int(score_m.group(1))
            away_score = int(score_m.group(2))
            if home_score == 0 and away_score == 0:
                continue  # future match
        else:
            continue  # no score

        results.append({
            "match_date": date_str,
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
        })

    return results


def import_web(conn, comp=None, season="x2526"):
    """Import standings, rosters, and match results for a competition.

    If comp is None, imports all competitions that have web data available.
    """
    comps = [comp] if comp else list(COMPETITIONS.keys())

    for c in comps:
        if c not in COMPETITIONS:
            print(f"  Ismeretlen bajnoksag: {c}, kihagyva")
            continue

        comp_name = COMPETITIONS[c][0]
        print(f"\n{'='*50}")
        print(f"Web scraping: {comp_name} ({c})")
        print(f"{'='*50}")

        # 1. Standings
        try:
            standings = scrape_standings(season, c)
            if standings:
                for s in standings:
                    conn.execute(
                        """INSERT OR REPLACE INTO standings
                           (comp_code, season, team_name, rank, games_played,
                            wins, losses, home_record, away_record, streak,
                            last_five, team_page_url, team_id)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (c, season, s["team_name"], s["rank"], s["gp"],
                         s["wins"], s["losses"], s["home_record"], s["away_record"],
                         s["streak"], s["last_five"], s["team_page_url"], s["team_id"]),
                    )
                conn.commit()
                print(f"  Standings: {len(standings)} csapat")
            else:
                print(f"  Standings: nem talalhato")
        except Exception as e:
            print(f"  Standings hiba: {e}")

        time.sleep(API_DELAY)

        # 2. Rosters (from standings team URLs)
        try:
            roster_count = 0
            standings_rows = conn.execute(
                "SELECT team_name, team_page_url, team_id FROM standings WHERE comp_code=? AND season=?",
                (c, season),
            ).fetchall()
            for row in standings_rows:
                team_name, team_url, team_id = row["team_name"], row["team_page_url"], row["team_id"]
                if not team_url:
                    continue
                try:
                    players = scrape_roster(team_url)
                    for p in players:
                        conn.execute(
                            """INSERT OR REPLACE INTO player_roster_meta
                               (comp_code, season, team_name, player_name,
                                jersey_number, position, height_cm, photo_url, birth_year)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (c, season, team_name, p["player_name"],
                             p["jersey_number"], p["position"], p["height_cm"],
                             p["photo_url"], p["birth_year"]),
                        )
                    roster_count += len(players)
                    time.sleep(API_DELAY)
                except Exception as e:
                    print(f"    Roster hiba ({team_name}): {e}")
            conn.commit()
            print(f"  Rosters: {roster_count} jatekos")
        except Exception as e:
            print(f"  Roster hiba: {e}")

        # 3. Match results (from each team's schedule page)
        try:
            result_count = 0
            seen = set()
            for row in standings_rows:
                team_name, team_url, team_id = row["team_name"], row["team_page_url"], row["team_id"]
                if not team_id:
                    continue
                try:
                    results = scrape_match_results(season, c, team_id)
                    for r in results:
                        key = (r["match_date"], r["home_team"], r["away_team"])
                        if key in seen:
                            continue
                        seen.add(key)
                        conn.execute(
                            """INSERT OR REPLACE INTO web_match_results
                               (comp_code, season, match_date, home_team, away_team,
                                home_score, away_score, team_id)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                            (c, season, r["match_date"], r["home_team"], r["away_team"],
                             r["home_score"], r["away_score"], team_id),
                        )
                        result_count += 1
                    time.sleep(API_DELAY)
                except Exception as e:
                    print(f"    Results hiba ({team_name}): {e}")
            conn.commit()
            print(f"  Match results: {result_count} meccs")
        except Exception as e:
            print(f"  Results hiba: {e}")

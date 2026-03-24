# mkosz-stats

Egységes MKOSZ kosárlabda statisztikai adatbázis — összefogja a scoresheet, play-by-play és shotchart adatforrásokat.

## Adatforrások

| Forrás | Bajnokságok | Típus |
|--------|-------------|-------|
| `mkosz-scoresheet` | NB2, Budapest regionális | PDF jegyzőkönyv → játékosstatisztika |
| `mkosz-play-by-play` | NB1B, MEFOB | Eseménylista → aggregált statisztika |
| Shotchart API | NB1B, Női NB1 | Dobástérkép (x/y koordináták) |

## Séma

Az `mkosz_stats.sqlite` egységes adatbázis dimenziós táblái:
- `seasons`, `competitions`, `teams`, `players` — dimenziók
- `matches` — meccsek (has_scoresheet, has_pbp, has_shotchart flagek)
- `player_game_stats` — játékosonkénti meccsstatisztika
- `pbp_events`, `substitutions` — PBP nyers adatok
- `shots` — shotchart dobások (x, y, result, playercode)

## Használat

```bash
pip install -r requirements.txt
python -m mkosz_stats          # Teljes pipeline futtatás
```

## Konfiguráció

A `config.py`-ban:
- `COMP_DATA_SOURCES` — bajnokságonként elérhető adatforrások
- `COMPETITIONS` — bajnokság metadata (szint, nem)
- Forrás DB útvonalak relatívak a parent könyvtárhoz (testvér repók)

## Player resolver

A `player_resolver.py` egyedi játékos-azonosítás (reconciliation):
1. Shotchart playercode regisztráció
2. NB1B PBP ↔ shotchart bridge (azonos meccsen name match)
3. Exact name propagáció más meccsekre
4. Fact table playercode kitöltés

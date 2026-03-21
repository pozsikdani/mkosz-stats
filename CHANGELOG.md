# Changelog

## 2026-03-21

### Fixed
- **Config paths**: `config.py` now resolves source DB paths relative to the parent directory (sibling repos) instead of hardcoding `~/Desktop/claudecode/`. Works as long as all repos live under the same parent (e.g. `~/MKOSZ/`).
- **PBP importer** (`importers/pbp.py`): removed dependency on non-existent `player_stats` table in `pbp.sqlite`. Now aggregates all player stats directly from the `events` table.
- **FG attempt tracking**: `_aggregate_basic_stats()` now counts missed field goals (`CLOSE_MISS`, `MID_MISS`, `THREE_MISS`, `DUNK_MISS`) to populate `fg2_attempted` and `fg3_attempted`, enabling FG% calculation.
- **Starter detection**: new `_is_starter()` function infers starting five from substitution and event sequence data (ported from `parse_pbp.py:get_starters()`).
- **Playing time**: new `_get_player_minutes()` function calculates approximate minutes played from substitution data (ported from `parse_pbp.py:get_playing_time()`).

### Changed
- `.gitignore`: removed `*.sqlite` so pre-computed databases can be committed to git.

### Data
- Initialized `mkosz_stats.sqlite` unified database:
  - 686 matches (372 scoresheet, 314 PBP)
  - 271 matches with shot chart data (36,967 shots)
  - 12,712 player game stats
  - 119,842 PBP events
  - 14,863 substitutions
  - 7 competitions (NB1 B + NB2)

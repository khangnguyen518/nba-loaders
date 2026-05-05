"""Tests for loaders/team_game_logs.py"""
from unittest.mock import MagicMock, patch
import pytest
from loaders.team_game_logs import TeamGameLogsLoader


def _team_log_response(season_id: str, game_ids: list[str]) -> MagicMock:
    headers = ["TEAM_ID", "TEAM_ABBREVIATION", "TEAM_NAME", "GAME_ID",
               "GAME_DATE", "MATCHUP", "WL", "PTS"]
    data = [
        [1610612747, "LAL", "Los Angeles Lakers", gid, "2025-01-10", "LAL vs DEN", "W", 112]
        for gid in game_ids
    ]
    mock = MagicMock()
    mock.league_game_log.get_dict.return_value = {"headers": headers, "data": data}
    return mock


# ---------------------------------------------------------------------------
# Season ID format
# ---------------------------------------------------------------------------

class TestSeasonIdFormat:

    def test_season_id_matches_yyyy_yy_format(self):
        loader = TeamGameLogsLoader(start_season=2024, end_season=2024)
        seen_season_ids = []

        def tracking(func, **kw):
            seen_season_ids.append(kw["season"])
            return _team_log_response(kw["season"], ["0022400001"])

        loader.api_call = tracking
        loader.fetch_data()

        assert seen_season_ids == ["2024-25"]

    def test_season_id_suffix_is_next_year(self):
        for year in [2019, 2022, 2023, 2024]:
            loader = TeamGameLogsLoader(start_season=year, end_season=year)
            seen = []
            loader.api_call = lambda func, season, **kw: (seen.append(season) or
                                                           _team_log_response(season, []))
            loader.fetch_data()
            sid = seen[0]
            start = int(sid.split("-")[0])
            suffix = int(sid.split("-")[1])
            assert suffix == (start + 1) % 100, f"Failed for year {year}: got {sid}"


# ---------------------------------------------------------------------------
# Season range
# ---------------------------------------------------------------------------

class TestSeasonRange:

    def test_correct_number_of_api_calls(self):
        loader = TeamGameLogsLoader(start_season=2022, end_season=2024)
        call_count = [0]

        def counting(func, **kw):
            call_count[0] += 1
            return _team_log_response(kw["season"], ["0022400001"])

        loader.api_call = counting
        loader.fetch_data()

        assert call_count[0] == 3  # 2022, 2023, 2024

    def test_all_seasons_in_range_fetched(self):
        loader = TeamGameLogsLoader(start_season=2022, end_season=2024)
        seen_seasons = []

        def tracking(func, **kw):
            seen_seasons.append(kw["season"])
            return _team_log_response(kw["season"], ["0022400001"])

        loader.api_call = tracking
        loader.fetch_data()

        assert seen_seasons == ["2022-23", "2023-24", "2024-25"]

    def test_single_season_range_one_call(self):
        loader = TeamGameLogsLoader(start_season=2024, end_season=2024)
        call_count = [0]

        def counting(func, **kw):
            call_count[0] += 1
            return _team_log_response(kw["season"], ["0022400001"])

        loader.api_call = counting
        loader.fetch_data()

        assert call_count[0] == 1


# ---------------------------------------------------------------------------
# SEASON_ID injection
# ---------------------------------------------------------------------------

class TestSeasonIdInjection:

    def test_season_id_injected_into_rows(self):
        loader = TeamGameLogsLoader(start_season=2024, end_season=2024)
        loader.api_call = lambda func, **kw: _team_log_response(kw["season"], ["0022400001", "0022400002"])

        rows = loader.fetch_data()
        assert all(r["SEASON_ID"] == "2024-25" for r in rows)

    def test_season_id_correct_across_multiple_seasons(self):
        loader = TeamGameLogsLoader(start_season=2023, end_season=2024)
        loader.api_call = lambda func, **kw: _team_log_response(kw["season"], ["0022400001"])

        rows = loader.fetch_data()
        season_ids = {r["SEASON_ID"] for r in rows}
        assert season_ids == {"2023-24", "2024-25"}


# ---------------------------------------------------------------------------
# API response handling
# ---------------------------------------------------------------------------

class TestApiHandling:

    def test_none_response_continues_to_next_season(self):
        loader = TeamGameLogsLoader(start_season=2023, end_season=2024)
        call_count = [0]

        def selective(func, **kw):
            call_count[0] += 1
            if kw["season"] == "2023-24":
                return None
            return _team_log_response(kw["season"], ["0022400001"])

        loader.api_call = selective
        rows = loader.fetch_data()

        assert call_count[0] == 2          # both seasons attempted
        assert len(rows) == 1              # only 2024-25 returned rows
        assert rows[0]["SEASON_ID"] == "2024-25"

    def test_all_none_responses_returns_empty_list(self):
        loader = TeamGameLogsLoader(start_season=2023, end_season=2024)
        loader.api_call = lambda func, **kw: None

        assert loader.fetch_data() == []

    def test_parse_error_skips_season_no_crash(self):
        loader = TeamGameLogsLoader(start_season=2024, end_season=2024)
        broken = MagicMock()
        broken.league_game_log.get_dict.side_effect = RuntimeError("malformed")
        loader.api_call = lambda func, **kw: broken

        rows = loader.fetch_data()  # must not raise
        assert rows == []

    def test_rows_from_all_seasons_combined(self):
        loader = TeamGameLogsLoader(start_season=2022, end_season=2024)
        loader.api_call = lambda func, **kw: _team_log_response(kw["season"], ["G1", "G2"])

        rows = loader.fetch_data()
        assert len(rows) == 6   # 3 seasons × 2 games each

    def test_nan_value_cleaned_to_none(self):
        loader = TeamGameLogsLoader(start_season=2024, end_season=2024)

        mock = MagicMock()
        mock.league_game_log.get_dict.return_value = {
            "headers": ["TEAM_ID", "GAME_ID", "PTS"],
            "data":    [[1610612747, "G1", float("nan")]],
        }
        loader.api_call = lambda func, **kw: mock

        rows = loader.fetch_data()
        assert rows[0]["PTS"] is None


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class TestConfig:

    def test_table_name(self):
        assert TeamGameLogsLoader().table_name == "raw_team_game_logs"

    def test_write_mode(self):
        assert TeamGameLogsLoader().write_mode == "upsert"

    def test_upsert_keys(self):
        assert TeamGameLogsLoader().upsert_keys == ["TEAM_ID", "GAME_ID"]

    def test_default_season_range_from_config(self):
        from config import START_SEASON, END_SEASON
        loader = TeamGameLogsLoader()
        assert loader.start_season == START_SEASON
        assert loader.end_season   == END_SEASON

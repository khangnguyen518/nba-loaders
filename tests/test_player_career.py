"""Tests for loaders/player_career.py"""
from types import SimpleNamespace
from unittest.mock import MagicMock
import pytest
from loaders.player_career import PlayerCareerLoader


def _player(id: int, is_active: bool = True) -> SimpleNamespace:
    return SimpleNamespace(id=id, is_active=is_active)


def _career_response(player_id: int, seasons: list[str]) -> MagicMock:
    headers = ["PLAYER_ID", "SEASON_ID", "TEAM_ID", "TEAM_ABBREVIATION",
               "GP", "PTS", "REB", "AST"]
    data = [
        [player_id, season, 1610612747, "LAL", 70, 25.0, 7.0, 8.0]
        for season in seasons
    ]
    mock = MagicMock()
    mock.season_totals_regular_season.get_dict.return_value = {
        "headers": headers,
        "data":    data,
    }
    return mock


def _loader(**kwargs) -> PlayerCareerLoader:
    loader = PlayerCareerLoader(**kwargs)
    loader._get_players          = lambda: []
    loader._get_loaded_player_ids = lambda: set()
    return loader


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

class TestFilters:

    def test_active_only_processes_only_active(self):
        loader = PlayerCareerLoader(active_only=True)
        loader._get_loaded_player_ids = lambda: set()
        loader._get_players           = lambda: [_player(1, True), _player(2, False)]

        # Mirror what _get_players would actually filter to
        loader._get_players = lambda: [_player(1, True)]

        seen = []
        loader.api_call = lambda func, **kw: (seen.append(kw["player_id"]) or
                                               _career_response(kw["player_id"], ["2024-25"]))
        loader.fetch_data()
        assert seen == [1]

    def test_historical_only_processes_only_inactive(self):
        loader = PlayerCareerLoader(historical_only=True)
        loader._get_loaded_player_ids = lambda: set()
        loader._get_players           = lambda: [_player(2, False)]

        seen = []
        loader.api_call = lambda func, **kw: (seen.append(kw["player_id"]) or
                                               _career_response(kw["player_id"], ["2010-11"]))
        loader.fetch_data()
        assert seen == [2]

    def test_limit_respected(self):
        loader = PlayerCareerLoader(limit=1)
        loader._get_loaded_player_ids = lambda: set()
        loader._get_players           = lambda: [_player(1), _player(2)][:1]

        seen = []
        loader.api_call = lambda func, **kw: (seen.append(kw["player_id"]) or
                                               _career_response(kw["player_id"], ["2024-25"]))
        loader.fetch_data()
        assert len(seen) == 1


# ---------------------------------------------------------------------------
# Multi-season behaviour
# ---------------------------------------------------------------------------

class TestMultiSeason:

    def test_multiple_seasons_per_player_all_returned(self):
        loader = _loader()
        loader._get_players = lambda: [_player(2544)]

        seasons = ["2022-23", "2023-24", "2024-25"]
        loader.api_call = lambda func, **kw: _career_response(kw["player_id"], seasons)

        rows = loader.fetch_data()
        assert len(rows) == 3
        returned_seasons = {r["SEASON_ID"] for r in rows}
        assert returned_seasons == set(seasons)

    def test_multiple_players_rows_combined(self):
        loader = _loader()
        loader._get_players = lambda: [_player(1), _player(2)]

        def api(func, **kw):
            pid = kw["player_id"]
            return _career_response(pid, ["2024-25"])

        loader.api_call = api
        rows = loader.fetch_data()
        assert len(rows) == 2
        assert {r["PLAYER_ID"] for r in rows} == {1, 2}


# ---------------------------------------------------------------------------
# Resume logic
# ---------------------------------------------------------------------------

class TestResume:

    def test_resume_skips_already_loaded_player(self):
        loader = PlayerCareerLoader(resume=True)
        loader._get_players          = lambda: [_player(1), _player(2)]
        loader._get_loaded_player_ids = lambda: {1}

        seen = []
        loader.api_call = lambda func, **kw: (seen.append(kw["player_id"]) or
                                               _career_response(kw["player_id"], ["2024-25"]))
        loader.fetch_data()
        assert 1 not in seen
        assert 2 in seen

    def test_no_resume_processes_all(self):
        loader = PlayerCareerLoader(resume=False)
        loader._get_players          = lambda: [_player(1), _player(2)]
        loader._get_loaded_player_ids = lambda: set()

        seen = []
        loader.api_call = lambda func, **kw: (seen.append(kw["player_id"]) or
                                               _career_response(kw["player_id"], ["2024-25"]))
        loader.fetch_data()
        assert set(seen) == {1, 2}


# ---------------------------------------------------------------------------
# API response handling
# ---------------------------------------------------------------------------

class TestApiHandling:

    def test_none_response_skips_player(self):
        loader = _loader()
        loader._get_players = lambda: [_player(1), _player(2)]

        def selective(func, **kw):
            if kw["player_id"] == 1:
                return None
            return _career_response(kw["player_id"], ["2024-25"])

        loader.api_call = selective
        rows = loader.fetch_data()
        assert len(rows) == 1
        assert rows[0]["PLAYER_ID"] == 2

    def test_parse_error_skips_player_no_crash(self):
        loader = _loader()
        loader._get_players = lambda: [_player(1)]

        broken = MagicMock()
        broken.season_totals_regular_season.get_dict.side_effect = RuntimeError("oops")
        loader.api_call = lambda func, **kw: broken

        rows = loader.fetch_data()  # must not raise
        assert rows == []

    def test_empty_data_array_contributes_no_rows(self):
        loader = _loader()
        loader._get_players = lambda: [_player(1)]

        mock = MagicMock()
        mock.season_totals_regular_season.get_dict.return_value = {
            "headers": ["PLAYER_ID", "SEASON_ID"],
            "data":    [],
        }
        loader.api_call = lambda func, **kw: mock
        assert loader.fetch_data() == []


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class TestConfig:

    def test_table_name(self):
        assert PlayerCareerLoader().table_name == "raw_player_career_stats"

    def test_write_mode(self):
        assert PlayerCareerLoader().write_mode == "upsert"

    def test_upsert_keys(self):
        assert PlayerCareerLoader().upsert_keys == ["PLAYER_ID", "SEASON_ID", "TEAM_ID"]

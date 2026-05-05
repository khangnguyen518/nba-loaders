"""Tests for loaders/game_logs.py"""
from unittest.mock import MagicMock, call
import pytest
from loaders.game_logs import GameLogsLoader


def _game_log_response(player_id: int, game_ids: list[str]) -> MagicMock:
    headers = ["Player_ID", "Game_ID", "SEASON_ID", "GAME_DATE", "MATCHUP",
               "WL", "MIN", "PTS", "REB", "AST"]
    data = [
        [player_id, gid, "22024", "2025-01-10", "LAL vs DEN", "W", 34.0, 28, 7, 9]
        for gid in game_ids
    ]
    mock = MagicMock()
    mock.player_game_log.get_dict.return_value = {"headers": headers, "data": data}
    return mock


def _loader(**kwargs) -> GameLogsLoader:
    loader = GameLogsLoader(**kwargs)
    loader._get_player_seasons = lambda: {}
    loader._get_loaded_keys    = lambda: set()
    return loader


# ---------------------------------------------------------------------------
# Season type behaviour
# ---------------------------------------------------------------------------

class TestSeasonTypes:

    def test_default_fetches_both_season_types(self):
        loader = _loader()
        loader._get_player_seasons = lambda: {2544: ["2024-25"]}

        seen_types = []
        def tracking(func, **kw):
            seen_types.append(kw["season_type_all_star"])
            return _game_log_response(kw["player_id"], ["0022400001"])

        loader.api_call = tracking
        loader.fetch_data()

        assert "Regular Season" in seen_types
        assert "Playoffs" in seen_types
        assert len(seen_types) == 2

    def test_regular_season_only_skips_playoffs(self):
        loader = _loader(season_type="Regular Season")
        loader._get_player_seasons = lambda: {2544: ["2024-25"]}

        seen_types = []
        def tracking(func, **kw):
            seen_types.append(kw["season_type_all_star"])
            return _game_log_response(kw["player_id"], ["0022400001"])

        loader.api_call = tracking
        loader.fetch_data()

        assert seen_types == ["Regular Season"]

    def test_playoffs_only_skips_regular_season(self):
        loader = _loader(season_type="Playoffs")
        loader._get_player_seasons = lambda: {2544: ["2024-25"]}

        seen_types = []
        def tracking(func, **kw):
            seen_types.append(kw["season_type_all_star"])
            return _game_log_response(kw["player_id"], ["0042400001"])

        loader.api_call = tracking
        loader.fetch_data()

        assert seen_types == ["Playoffs"]

    def test_season_type_injected_into_rows(self):
        loader = _loader(season_type="Regular Season")
        loader._get_player_seasons = lambda: {2544: ["2024-25"]}
        loader.api_call = lambda func, **kw: _game_log_response(kw["player_id"], ["0022400001"])

        rows = loader.fetch_data()
        assert all(r["season_type"] == "Regular Season" for r in rows)

    def test_both_season_types_tagged_in_rows(self):
        loader = _loader()
        loader._get_player_seasons = lambda: {2544: ["2024-25"]}
        loader.api_call = lambda func, **kw: _game_log_response(kw["player_id"], ["0022400001"])

        rows = loader.fetch_data()
        season_types_in_rows = {r["season_type"] for r in rows}
        assert season_types_in_rows == {"Regular Season", "Playoffs"}


# ---------------------------------------------------------------------------
# Multiple seasons per player
# ---------------------------------------------------------------------------

class TestMultiSeason:

    def test_all_seasons_fetched_for_player(self):
        loader = _loader()
        loader._get_player_seasons = lambda: {2544: ["2022-23", "2023-24", "2024-25"]}

        seen_seasons = []
        def tracking(func, **kw):
            seen_seasons.append(kw["season"])
            return _game_log_response(kw["player_id"], ["0022400001"])

        loader.api_call = tracking
        loader.fetch_data()

        # 3 seasons × 2 season types = 6 calls
        assert len(seen_seasons) == 6
        assert "2022-23" in seen_seasons
        assert "2023-24" in seen_seasons
        assert "2024-25" in seen_seasons

    def test_multiple_players_all_processed(self):
        loader = _loader()
        loader._get_player_seasons = lambda: {
            2544:   ["2024-25"],
            203999: ["2024-25"],
        }
        loader.api_call = lambda func, **kw: _game_log_response(kw["player_id"], ["0022400001"])

        rows = loader.fetch_data()
        player_ids = {r["Player_ID"] for r in rows}
        assert player_ids == {2544, 203999}


# ---------------------------------------------------------------------------
# Resume logic
# ---------------------------------------------------------------------------

class TestResume:

    def test_resume_skips_already_loaded_player(self):
        loader = _loader(resume=True)
        loader._get_player_seasons = lambda: {1: ["2024-25"], 2: ["2024-25"]}
        loader._get_loaded_keys    = lambda: {1}

        seen = []
        def tracking(func, **kw):
            seen.append(kw["player_id"])
            return _game_log_response(kw["player_id"], ["0022400001"])

        loader.api_call = tracking
        loader.fetch_data()

        assert 1 not in seen
        assert 2 in seen

    def test_no_resume_processes_all_players(self):
        loader = _loader(resume=False)
        loader._get_player_seasons = lambda: {1: ["2024-25"], 2: ["2024-25"]}
        loader._get_loaded_keys    = lambda: set()

        seen = []
        def tracking(func, **kw):
            seen.append(kw["player_id"])
            return _game_log_response(kw["player_id"], ["0022400001"])

        loader.api_call = tracking
        loader.fetch_data()

        assert set(seen) == {1, 2}


# ---------------------------------------------------------------------------
# API response handling
# ---------------------------------------------------------------------------

class TestApiHandling:

    def test_none_response_continues_to_next_season_type(self):
        loader = _loader()
        loader._get_player_seasons = lambda: {2544: ["2024-25"]}

        call_count = [0]
        def selective(func, **kw):
            call_count[0] += 1
            if kw["season_type_all_star"] == "Regular Season":
                return None
            return _game_log_response(kw["player_id"], ["0042400001"])

        loader.api_call = selective
        rows = loader.fetch_data()

        assert call_count[0] == 2           # both season types attempted
        assert len(rows) == 1               # only Playoffs row came back
        assert rows[0]["season_type"] == "Playoffs"

    def test_all_none_responses_returns_empty_list(self):
        loader = _loader()
        loader._get_player_seasons = lambda: {2544: ["2024-25"]}
        loader.api_call            = lambda func, **kw: None

        assert loader.fetch_data() == []

    def test_parse_error_skips_without_crash(self):
        loader = _loader()
        loader._get_player_seasons = lambda: {2544: ["2024-25"]}

        broken = MagicMock()
        broken.player_game_log.get_dict.side_effect = RuntimeError("bad")
        loader.api_call = lambda func, **kw: broken

        rows = loader.fetch_data()  # must not raise
        assert rows == []


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class TestConfig:

    def test_table_name(self):
        assert GameLogsLoader().table_name == "raw_player_game_logs"

    def test_write_mode(self):
        assert GameLogsLoader().write_mode == "upsert"

    def test_upsert_keys(self):
        assert GameLogsLoader().upsert_keys == ["Player_ID", "Game_ID", "season_type"]

    def test_default_season_range_from_config(self):
        from config import START_SEASON, END_SEASON
        loader = GameLogsLoader()
        assert loader.start_season == START_SEASON
        assert loader.end_season   == END_SEASON

    def test_custom_season_range_stored(self):
        loader = GameLogsLoader(start_season=2010, end_season=2015)
        assert loader.start_season == 2010
        assert loader.end_season   == 2015

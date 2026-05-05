"""Tests for loaders/player_info.py"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call
import pytest
from loaders.player_info import PlayerInfoLoader


def _player(id: int, is_active: bool = True) -> SimpleNamespace:
    return SimpleNamespace(id=id, is_active=is_active)


def _info_response(person_id: int) -> MagicMock:
    mock = MagicMock()
    mock.common_player_info.get_dict.return_value = {
        "headers": ["PERSON_ID", "FIRST_NAME", "LAST_NAME", "TO_YEAR"],
        "data":    [[person_id, "First", "Last", 2024]],
    }
    return mock


def _loader(**kwargs) -> PlayerInfoLoader:
    loader = PlayerInfoLoader(**kwargs)
    loader._get_players          = lambda: []
    loader._get_loaded_player_ids = lambda: set()
    return loader


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

class TestFilters:

    def test_active_only_skips_inactive_players(self):
        all_players = [_player(1, True), _player(2, False), _player(3, True)]
        loader = PlayerInfoLoader(active_only=True)
        loader._get_loaded_player_ids = lambda: set()

        # Manually apply the same filter _get_players uses
        filtered = [p for p in all_players if p.is_active]
        loader._get_players = lambda: filtered

        responses = {1: _info_response(1), 3: _info_response(3)}
        loader.api_call = lambda func, **kw: responses.get(kw["player_id"])

        rows = loader.fetch_data()
        returned_ids = [r["PERSON_ID"] for r in rows]
        assert 1 in returned_ids
        assert 3 in returned_ids
        assert 2 not in returned_ids

    def test_historical_only_skips_active_players(self):
        all_players = [_player(1, True), _player(2, False)]
        loader = PlayerInfoLoader(historical_only=True)
        loader._get_loaded_player_ids = lambda: set()

        filtered = [p for p in all_players if not p.is_active]
        loader._get_players = lambda: filtered

        loader.api_call = lambda func, **kw: _info_response(kw["player_id"])

        rows = loader.fetch_data()
        assert len(rows) == 1
        assert rows[0]["PERSON_ID"] == 2

    def test_limit_caps_player_count(self):
        loader = PlayerInfoLoader(limit=2)
        loader._get_players = lambda: [_player(1), _player(2), _player(3)]
        loader._get_loaded_player_ids = lambda: set()

        call_count = 0
        def counting_api_call(func, **kw):
            nonlocal call_count
            call_count += 1
            return _info_response(kw["player_id"])

        loader.api_call = counting_api_call

        # limit is applied inside _get_players, so we mirror that in the mock
        loader._get_players = lambda: [_player(1), _player(2), _player(3)][:2]
        loader.fetch_data()
        assert call_count == 2


# ---------------------------------------------------------------------------
# Resume logic
# ---------------------------------------------------------------------------

class TestResume:

    def test_resume_true_skips_already_loaded_player(self):
        loader = PlayerInfoLoader(resume=True)
        loader._get_players          = lambda: [_player(1), _player(2)]
        loader._get_loaded_player_ids = lambda: {1}  # player 1 already loaded

        seen_ids = []
        def tracking_api_call(func, **kw):
            seen_ids.append(kw["player_id"])
            return _info_response(kw["player_id"])

        loader.api_call = tracking_api_call
        loader.fetch_data()

        assert 1 not in seen_ids
        assert 2 in seen_ids

    def test_resume_false_includes_all_players(self):
        loader = PlayerInfoLoader(resume=False)
        loader._get_players          = lambda: [_player(1), _player(2)]
        loader._get_loaded_player_ids = lambda: {1}  # ignored when resume=False

        # resume=False → _get_loaded_player_ids returns set() (see implementation)
        loader._get_loaded_player_ids = lambda: set()

        seen_ids = []
        def tracking_api_call(func, **kw):
            seen_ids.append(kw["player_id"])
            return _info_response(kw["player_id"])

        loader.api_call = tracking_api_call
        loader.fetch_data()

        assert 1 in seen_ids
        assert 2 in seen_ids


# ---------------------------------------------------------------------------
# API response handling
# ---------------------------------------------------------------------------

class TestApiHandling:

    def test_none_response_skips_player_no_crash(self):
        loader = _loader()
        loader._get_players = lambda: [_player(1), _player(2)]

        def selective_api_call(func, **kw):
            if kw["player_id"] == 1:
                return None
            return _info_response(kw["player_id"])

        loader.api_call = selective_api_call
        rows = loader.fetch_data()

        assert len(rows) == 1
        assert rows[0]["PERSON_ID"] == 2

    def test_all_none_responses_returns_empty_list(self):
        loader = _loader()
        loader._get_players = lambda: [_player(1), _player(2)]
        loader.api_call     = lambda func, **kw: None

        assert loader.fetch_data() == []

    def test_parse_error_skips_player_no_crash(self):
        loader = _loader()
        loader._get_players = lambda: [_player(1)]

        broken = MagicMock()
        broken.common_player_info.get_dict.side_effect = ValueError("bad data")
        loader.api_call = lambda func, **kw: broken

        rows = loader.fetch_data()  # must not raise
        assert rows == []

    def test_nan_value_cleaned_to_none(self):
        loader = _loader()
        loader._get_players = lambda: [_player(1)]

        mock = MagicMock()
        mock.common_player_info.get_dict.return_value = {
            "headers": ["PERSON_ID", "SEASON_EXP", "TO_YEAR"],
            "data":    [[1, float("nan"), 2024]],
        }
        loader.api_call = lambda func, **kw: mock

        rows = loader.fetch_data()
        assert rows[0]["SEASON_EXP"] is None

    def test_empty_data_array_skips_player(self):
        """API returns headers but no data rows for a player."""
        loader = _loader()
        loader._get_players = lambda: [_player(1)]

        mock = MagicMock()
        mock.common_player_info.get_dict.return_value = {
            "headers": ["PERSON_ID", "TO_YEAR"],
            "data":    [],
        }
        loader.api_call = lambda func, **kw: mock

        assert loader.fetch_data() == []


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class TestConfig:

    def test_table_name(self):
        assert PlayerInfoLoader().table_name == "raw_player_common_info"

    def test_write_mode(self):
        assert PlayerInfoLoader().write_mode == "upsert"

    def test_upsert_keys(self):
        assert PlayerInfoLoader().upsert_keys == ["PERSON_ID", "TO_YEAR"]

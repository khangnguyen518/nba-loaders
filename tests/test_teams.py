"""Tests for loaders/teams.py"""
from unittest.mock import patch
import pytest
from loaders.teams import TeamsLoader

SAMPLE_TEAMS = [
    {"id": 1610612747, "full_name": "Los Angeles Lakers",  "abbreviation": "LAL", "nickname": "Lakers",  "city": "Los Angeles",  "state": "California", "year_founded": 1946},
    {"id": 1610612744, "full_name": "Golden State Warriors","abbreviation": "GSW", "nickname": "Warriors","city": "San Francisco","state": "California", "year_founded": 1946},
]


class TestFetchData:

    def test_happy_path_returns_all_rows(self):
        loader = TeamsLoader()
        with patch.object(loader, "api_call", return_value=SAMPLE_TEAMS):
            assert loader.fetch_data() == SAMPLE_TEAMS

    def test_returns_correct_row_count(self):
        loader = TeamsLoader()
        with patch.object(loader, "api_call", return_value=SAMPLE_TEAMS):
            assert len(loader.fetch_data()) == 2

    def test_none_response_returns_empty_list(self):
        loader = TeamsLoader()
        with patch.object(loader, "api_call", return_value=None):
            assert loader.fetch_data() == []

    def test_empty_list_returns_empty_list(self):
        loader = TeamsLoader()
        with patch.object(loader, "api_call", return_value=[]):
            assert loader.fetch_data() == []

    def test_row_values_pass_through_unchanged(self):
        loader = TeamsLoader()
        with patch.object(loader, "api_call", return_value=SAMPLE_TEAMS):
            rows = loader.fetch_data()
        assert rows[0]["id"] == 1610612747
        assert rows[0]["abbreviation"] == "LAL"
        assert rows[0]["year_founded"] == 1946


class TestConfig:

    def test_table_name(self):
        assert TeamsLoader().table_name == "raw_teams"

    def test_write_mode_is_truncate(self):
        assert TeamsLoader().write_mode == "truncate"


class TestRunIntegration:

    @patch("loaders.base.get_bq_client")
    def test_run_writes_to_bigquery(self, mock_get_client, bq_client):
        mock_get_client.return_value = bq_client
        loader = TeamsLoader()
        with patch.object(loader, "api_call", return_value=SAMPLE_TEAMS):
            loader.run()
        bq_client.load_table_from_json.assert_called()

    @patch("loaders.base.get_bq_client")
    def test_run_empty_response_skips_write(self, mock_get_client, bq_client):
        mock_get_client.return_value = bq_client
        loader = TeamsLoader()
        with patch.object(loader, "api_call", return_value=[]):
            loader.run()
        bq_client.load_table_from_json.assert_not_called()

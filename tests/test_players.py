"""Tests for loaders/players.py"""
from unittest.mock import patch
import pytest
from loaders.players import PlayersLoader

SAMPLE_PLAYERS = [
    {"id": 2544,   "full_name": "LeBron James",   "first_name": "LeBron",   "last_name": "James",  "is_active": True},
    {"id": 201939, "full_name": "Stephen Curry",  "first_name": "Stephen",  "last_name": "Curry",  "is_active": True},
    {"id": 977,    "full_name": "Kobe Bryant",    "first_name": "Kobe",     "last_name": "Bryant", "is_active": False},
]


class TestFetchData:

    def test_happy_path_returns_all_rows(self):
        loader = PlayersLoader()
        with patch.object(loader, "api_call", return_value=SAMPLE_PLAYERS):
            assert loader.fetch_data() == SAMPLE_PLAYERS

    def test_returns_correct_row_count(self):
        loader = PlayersLoader()
        with patch.object(loader, "api_call", return_value=SAMPLE_PLAYERS):
            assert len(loader.fetch_data()) == 3

    def test_none_response_returns_empty_list(self):
        loader = PlayersLoader()
        with patch.object(loader, "api_call", return_value=None):
            assert loader.fetch_data() == []

    def test_empty_list_returns_empty_list(self):
        loader = PlayersLoader()
        with patch.object(loader, "api_call", return_value=[]):
            assert loader.fetch_data() == []

    def test_row_values_pass_through_unchanged(self):
        loader = PlayersLoader()
        with patch.object(loader, "api_call", return_value=SAMPLE_PLAYERS):
            rows = loader.fetch_data()
        assert rows[0]["id"] == 2544
        assert rows[0]["full_name"] == "LeBron James"
        assert rows[0]["is_active"] is True


class TestConfig:

    def test_table_name(self):
        assert PlayersLoader().table_name == "raw_players"

    def test_write_mode_is_truncate(self):
        assert PlayersLoader().write_mode == "truncate"


class TestRunIntegration:

    @patch("loaders.base.get_bq_client")
    def test_run_writes_to_bigquery(self, mock_get_client, bq_client):
        mock_get_client.return_value = bq_client
        loader = PlayersLoader()
        with patch.object(loader, "api_call", return_value=SAMPLE_PLAYERS):
            loader.run()
        bq_client.load_table_from_json.assert_called()

    @patch("loaders.base.get_bq_client")
    def test_run_empty_response_skips_write(self, mock_get_client, bq_client):
        mock_get_client.return_value = bq_client
        loader = PlayersLoader()
        with patch.object(loader, "api_call", return_value=[]):
            loader.run()
        bq_client.load_table_from_json.assert_not_called()

    @patch("loaders.base.get_bq_client")
    def test_run_none_response_skips_write(self, mock_get_client, bq_client):
        mock_get_client.return_value = bq_client
        loader = PlayersLoader()
        with patch.object(loader, "api_call", return_value=None):
            loader.run()
        bq_client.load_table_from_json.assert_not_called()

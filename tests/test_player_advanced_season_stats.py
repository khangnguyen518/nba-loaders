"""
Unit tests for loaders/player_advanced_season_stats.py

Run all tests (no BigQuery required):
    cd nba && source venv/bin/activate && python -m pytest tests/ -v

Smoke test against real BigQuery (requires .env + keyfile):
    cd nba && source venv/bin/activate && python -c "
    from loaders.player_advanced_season_stats import load_player_advanced_season_stats
    load_player_advanced_season_stats(season='2024-25')
    "
"""
import re
from unittest.mock import MagicMock, patch

import pytest

from loaders.player_advanced_season_stats import (
    CURRENT_SEASON,
    KEEP_COLUMNS,
    PlayerAdvancedSeasonStatsLoader,
)


# ---------------------------------------------------------------------------
# Shared fixtures & helpers
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Kill all sleeps in base.py so tests run instantly."""
    monkeypatch.setattr("loaders.base.time.sleep", lambda _: None)
    monkeypatch.setattr("loaders.base.random.uniform", lambda a, b: 0.0)


def _make_api_response(headers: list, rows: list) -> MagicMock:
    """Build a mock LeagueDashPlayerStats response."""
    mock = MagicMock()
    mock.league_dash_player_stats.get_dict.return_value = {
        "headers": headers,
        "data": rows,
    }
    return mock


def _make_bq_client() -> MagicMock:
    """Return a BigQuery client mock that satisfies create_table + upsert."""
    client = MagicMock()

    load_job = MagicMock()
    load_job.result.return_value = None
    client.load_table_from_json.return_value = load_job

    query_job = MagicMock()
    query_job.result.return_value = None
    client.query.return_value = query_job

    table = MagicMock()
    table.schema = []
    client.get_table.return_value = table

    return client


# All headers the real API currently returns for Advanced measure type
_HEADERS = [
    "PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "TEAM_ABBREVIATION", "AGE",
    "GP", "W", "L", "MIN",
    "PER", "USG_PCT", "PACE", "PIE",
    "BPM", "OBPM", "DBPM", "VORP",
    "OFF_RATING", "DEF_RATING", "NET_RATING",
    "AST_PCT", "AST_TO", "REB_PCT", "OREB_PCT", "DREB_PCT",
]

# One canonical row matching _HEADERS order
_ROW = [
    2544, "LeBron James", 1610612747, "LAL", 39.0,
    71, 47, 24, 35.3,
    24.8, 0.315, 101.2, 0.178,
    4.1, 3.8, 0.3, 4.2,
    115.2, 113.5, 1.7,
    0.391, 2.8, 0.142, 0.048, 0.237,
]


# ---------------------------------------------------------------------------
# fetch_data — happy path
# ---------------------------------------------------------------------------

class TestFetchDataHappyPath:

    def test_returns_one_row_per_player(self):
        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        mock_response = _make_api_response(_HEADERS, [_ROW, _ROW])

        with patch.object(loader, "api_call", return_value=mock_response):
            rows = loader.fetch_data()

        assert len(rows) == 2

    def test_only_keep_columns_plus_season_id_present(self):
        """No extra columns should leak through — only KEEP_COLUMNS ∪ {SEASON_ID}."""
        headers_with_extras = _HEADERS + ["W_PCT", "RANDOM_BASE_COL"]
        row_with_extras = _ROW + [0.661, 0.123]

        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        mock_response = _make_api_response(headers_with_extras, [row_with_extras])

        with patch.object(loader, "api_call", return_value=mock_response):
            rows = loader.fetch_data()

        assert set(rows[0].keys()) == KEEP_COLUMNS | {"SEASON_ID"}

    def test_values_pass_through_unchanged(self):
        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        mock_response = _make_api_response(_HEADERS, [_ROW])

        with patch.object(loader, "api_call", return_value=mock_response):
            rows = loader.fetch_data()

        row = rows[0]
        assert row["PLAYER_ID"] == 2544
        assert row["PLAYER_NAME"] == "LeBron James"
        assert row["PER"] == 24.8
        assert row["NET_RATING"] == 1.7

    def test_season_id_injected_from_constructor(self):
        loader = PlayerAdvancedSeasonStatsLoader(season="2023-24")
        mock_response = _make_api_response(_HEADERS, [_ROW])

        with patch.object(loader, "api_call", return_value=mock_response):
            rows = loader.fetch_data()

        assert rows[0]["SEASON_ID"] == "2023-24"


# ---------------------------------------------------------------------------
# fetch_data — empty / null responses
# ---------------------------------------------------------------------------

class TestFetchDataEmpty:

    def test_empty_data_array_returns_empty_list(self):
        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        mock_response = _make_api_response(_HEADERS, [])

        with patch.object(loader, "api_call", return_value=mock_response):
            rows = loader.fetch_data()

        assert rows == []

    def test_none_api_response_returns_empty_list(self):
        """api_call returns None (network failure, rate-limit, shutdown)."""
        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")

        with patch.object(loader, "api_call", return_value=None):
            rows = loader.fetch_data()

        assert rows == []

    def test_empty_response_does_not_raise(self):
        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        mock_response = _make_api_response(_HEADERS, [])

        with patch.object(loader, "api_call", return_value=mock_response):
            result = loader.fetch_data()  # must not raise

        assert result == []


# ---------------------------------------------------------------------------
# fetch_data — null / NaN advanced columns
# ---------------------------------------------------------------------------

class TestFetchDataNullColumns:
    """Players with 0 MIN have undefined rate stats (PER, USG_PCT, etc.)."""

    def _row_with(self, col: str, val):
        row = list(_ROW)
        row[_HEADERS.index(col)] = val
        return row

    def test_null_per_preserved_as_none(self):
        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        row = self._row_with("PER", None)
        mock_response = _make_api_response(_HEADERS, [row])

        with patch.object(loader, "api_call", return_value=mock_response):
            rows = loader.fetch_data()

        assert "PER" in rows[0]
        assert rows[0]["PER"] is None

    def test_nan_per_cleaned_to_none(self):
        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        row = self._row_with("PER", float("nan"))
        mock_response = _make_api_response(_HEADERS, [row])

        with patch.object(loader, "api_call", return_value=mock_response):
            rows = loader.fetch_data()

        assert rows[0]["PER"] is None

    def test_inf_column_cleaned_to_none(self):
        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        row = self._row_with("AST_TO", float("inf"))
        mock_response = _make_api_response(_HEADERS, [row])

        with patch.object(loader, "api_call", return_value=mock_response):
            rows = loader.fetch_data()

        assert rows[0]["AST_TO"] is None

    def test_all_advanced_cols_null_row_still_returned(self):
        """A row where every rate stat is None must still appear in output."""
        rate_cols = {"PER", "USG_PCT", "PACE", "PIE", "BPM", "OBPM", "DBPM",
                     "VORP", "OFF_RATING", "DEF_RATING", "NET_RATING",
                     "AST_PCT", "AST_TO", "REB_PCT", "OREB_PCT", "DREB_PCT"}
        row = list(_ROW)
        for col in rate_cols:
            row[_HEADERS.index(col)] = None

        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        mock_response = _make_api_response(_HEADERS, [row])

        with patch.object(loader, "api_call", return_value=mock_response):
            rows = loader.fetch_data()

        assert len(rows) == 1
        for col in rate_cols:
            assert rows[0][col] is None


# ---------------------------------------------------------------------------
# fetch_data — schema drift
# ---------------------------------------------------------------------------

class TestSchemaDrift:

    def test_extra_column_silently_dropped(self):
        headers = _HEADERS + ["NEW_METRIC_2025"]
        row = _ROW + [999.0]

        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        mock_response = _make_api_response(headers, [row])

        with patch.object(loader, "api_call", return_value=mock_response):
            rows = loader.fetch_data()

        assert "NEW_METRIC_2025" not in rows[0]

    def test_multiple_unknown_columns_all_dropped(self):
        headers = _HEADERS + ["COL_A", "COL_B", "COL_C"]
        row = _ROW + [1, 2, 3]

        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        mock_response = _make_api_response(headers, [row])

        with patch.object(loader, "api_call", return_value=mock_response):
            rows = loader.fetch_data()

        for col in ("COL_A", "COL_B", "COL_C"):
            assert col not in rows[0]

    def test_schema_drift_does_not_raise(self):
        headers = _HEADERS + ["SPONSOR_LOGO", "NFT_LINK"]
        row = _ROW + ["Nike", "https://example.com"]

        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        mock_response = _make_api_response(headers, [row])

        with patch.object(loader, "api_call", return_value=mock_response):
            rows = loader.fetch_data()  # must not raise

        assert len(rows) == 1

    def test_known_columns_unaffected_by_drift(self):
        """KEEP_COLUMNS values are correct even when extras are present."""
        headers = _HEADERS + ["DRIFT_COL"]
        row = _ROW + [0.0]

        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        mock_response = _make_api_response(headers, [row])

        with patch.object(loader, "api_call", return_value=mock_response):
            rows = loader.fetch_data()

        assert rows[0]["PLAYER_ID"] == 2544
        assert rows[0]["PER"] == 24.8


# ---------------------------------------------------------------------------
# Season parameter
# ---------------------------------------------------------------------------

class TestSeasonParam:

    def test_current_season_matches_yyyy_yy_format(self):
        assert re.match(r"^\d{4}-\d{2}$", CURRENT_SEASON), (
            f"CURRENT_SEASON '{CURRENT_SEASON}' does not match YYYY-YY"
        )

    def test_current_season_suffix_is_next_year(self):
        """E.g. '2025-26': the suffix 26 == (2025 + 1) % 100."""
        start = int(CURRENT_SEASON.split("-")[0])
        suffix = int(CURRENT_SEASON.split("-")[1])
        assert suffix == (start + 1) % 100

    def test_current_season_year_is_plausible(self):
        year = int(CURRENT_SEASON.split("-")[0])
        assert 2000 <= year <= 2100

    def test_default_loader_uses_current_season(self):
        loader = PlayerAdvancedSeasonStatsLoader()
        assert loader.season == CURRENT_SEASON

    def test_custom_season_stored_on_loader(self):
        loader = PlayerAdvancedSeasonStatsLoader(season="2019-20")
        assert loader.season == "2019-20"

    def test_custom_season_injected_into_rows(self):
        loader = PlayerAdvancedSeasonStatsLoader(season="2018-19")
        mock_response = _make_api_response(_HEADERS, [_ROW])

        with patch.object(loader, "api_call", return_value=mock_response):
            rows = loader.fetch_data()

        assert rows[0]["SEASON_ID"] == "2018-19"


# ---------------------------------------------------------------------------
# run() integration — mocked BigQuery
# ---------------------------------------------------------------------------

class TestRunIntegration:

    @patch("loaders.base.get_bq_client")
    def test_run_happy_path_writes_to_bigquery(self, mock_get_client):
        """Valid rows must reach load_table_from_json."""
        mock_get_client.return_value = _make_bq_client()
        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        mock_response = _make_api_response(_HEADERS, [_ROW])

        with patch.object(loader, "api_call", return_value=mock_response):
            loader.run()

        mock_client = mock_get_client.return_value
        mock_client.load_table_from_json.assert_called()

    @patch("loaders.base.get_bq_client")
    def test_run_empty_rows_skips_bq_write(self, mock_get_client):
        """Zero rows must not call load_table_from_json (avoid empty upsert)."""
        mock_client = _make_bq_client()
        mock_get_client.return_value = mock_client
        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        mock_response = _make_api_response(_HEADERS, [])

        with patch.object(loader, "api_call", return_value=mock_response):
            loader.run()

        mock_client.load_table_from_json.assert_not_called()

    @patch("loaders.base.get_bq_client")
    def test_run_none_response_skips_bq_write(self, mock_get_client):
        """None from api_call (network error) must not write to BigQuery."""
        mock_client = _make_bq_client()
        mock_get_client.return_value = mock_client
        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")

        with patch.object(loader, "api_call", return_value=None):
            loader.run()

        mock_client.load_table_from_json.assert_not_called()

    @patch("loaders.base.get_bq_client")
    def test_run_does_not_raise_on_empty_response(self, mock_get_client):
        mock_get_client.return_value = _make_bq_client()
        loader = PlayerAdvancedSeasonStatsLoader(season="2024-25")
        mock_response = _make_api_response(_HEADERS, [])

        with patch.object(loader, "api_call", return_value=mock_response):
            loader.run()  # must not raise

    @patch("loaders.base.get_bq_client")
    def test_upsert_keys_configured_correctly(self, mock_get_client):
        """Smoke-check the loader's upsert key config before any network call."""
        mock_get_client.return_value = _make_bq_client()
        loader = PlayerAdvancedSeasonStatsLoader()

        assert loader.write_mode == "upsert"
        assert loader.upsert_keys == ["PLAYER_ID", "SEASON_ID", "TEAM_ID"]
        assert loader.table_name == "raw_player_advanced_season_stats"

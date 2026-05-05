import sys
import os
from unittest.mock import MagicMock
import pytest

# Ensure the nba/ package root is on sys.path so `import loaders` works
# regardless of where pytest is invoked from.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Kill all sleeps in base.py so tests run instantly."""
    monkeypatch.setattr("loaders.base.time.sleep", lambda _: None)
    monkeypatch.setattr("loaders.base.random.uniform", lambda a, b: 0.0)


@pytest.fixture
def bq_client():
    """Stubbed BigQuery client that satisfies create_table + write + upsert."""
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

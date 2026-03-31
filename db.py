from google.cloud import bigquery
from google.oauth2 import service_account
from config import BQ_PROJECT, BQ_KEYFILE


def test_connection() -> bool:
    try:
        credentials = service_account.Credentials.from_service_account_file(BQ_KEYFILE)
        client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)
        client.query("SELECT 1").result()
        print("✓ BigQuery connection successful")
        return True
    except Exception as e:
        print(f"❌ BigQuery connection failed: {e}")
        return False
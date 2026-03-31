import os
from dotenv import load_dotenv

load_dotenv()

BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET", "nba_raw")
BQ_KEYFILE = os.getenv("BQ_KEYFILE", "bigquery-keyfile.json")

API_RATE_LIMIT    = 1    # seconds between API calls
API_TIMEOUT       = 15   # request timeout in seconds
API_MAX_RETRIES   = 5    # retry attempts before giving up

COOLDOWN_INTERVAL = 20   # take a break every N players
COOLDOWN_TIME     = 15   # seconds to wait during cooldown

START_SEASON = 1960      # default start season for game logs
END_SEASON   = 2025      # default end season for game logs

BATCH_SIZE = 1000         # rows per BigQuery insert batch
VERBOSE    = True        # print detailed logs
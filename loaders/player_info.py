import time
from nba_api.stats.endpoints import commonplayerinfo
from loaders.base import BaseLoader
from config import COOLDOWN_INTERVAL, COOLDOWN_TIME, VERBOSE
from google.cloud import bigquery
from google.oauth2 import service_account
from config import BQ_PROJECT, BQ_DATASET, BQ_KEYFILE


class PlayerInfoLoader(BaseLoader):

    def __init__(self, active_only=False, historical_only=False, limit=None, resume=False):
        super().__init__()
        self.table_name      = "raw_player_common_info"
        self.active_only     = active_only
        self.historical_only = historical_only
        self.limit           = limit
        self.resume          = resume
        self.write_mode      = 'upsert'
        self.upsert_keys     = ['PERSON_ID', 'TO_YEAR']

    def get_create_table_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS `{self.dataset}.{self.table_name}` (
            PERSON_ID                           INT64,
            FIRST_NAME                          STRING,
            LAST_NAME                           STRING,
            DISPLAY_FIRST_LAST                  STRING,
            DISPLAY_LAST_COMMA_FIRST            STRING,
            DISPLAY_FI_LAST                     STRING,
            PLAYER_SLUG                         STRING,
            BIRTHDATE                           STRING,
            SCHOOL                              STRING,
            COUNTRY                             STRING,
            LAST_AFFILIATION                    STRING,
            HEIGHT                              STRING,
            WEIGHT                              STRING,
            SEASON_EXP                          INT64,
            JERSEY                              STRING,
            POSITION                            STRING,
            ROSTERSTATUS                        STRING,
            GAMES_PLAYED_CURRENT_SEASON_FLAG    STRING,
            TEAM_ID                             INT64,
            TEAM_NAME                           STRING,
            TEAM_ABBREVIATION                   STRING,
            TEAM_CODE                           STRING,
            TEAM_CITY                           STRING,
            PLAYERCODE                          STRING,
            FROM_YEAR                           INT64,
            TO_YEAR                             INT64,
            DLEAGUE_FLAG                        STRING,
            NBA_FLAG                            STRING,
            GAMES_PLAYED_FLAG                   STRING,
            DRAFT_YEAR                          STRING,
            DRAFT_ROUND                         STRING,
            DRAFT_NUMBER                        STRING,
            GREATEST_75_FLAG                    STRING,
            loaded_at                           TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """

    def _get_loaded_player_ids(self) -> set:
        if not self.resume:
            return set()
        try:
            credentials = service_account.Credentials.from_service_account_file(BQ_KEYFILE)
            client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)
            query  = f"SELECT DISTINCT PERSON_ID FROM `{BQ_PROJECT}.{BQ_DATASET}.{self.table_name}`"
            result = client.query(query).result()
            return {row.PERSON_ID for row in result}
        except Exception:
            return set()

    def _get_players(self) -> list:
        credentials = service_account.Credentials.from_service_account_file(BQ_KEYFILE)
        client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)
        query  = f"SELECT id, is_active FROM `{BQ_PROJECT}.{BQ_DATASET}.raw_players`"
        rows   = list(client.query(query).result())

        if self.active_only:
            rows = [r for r in rows if r.is_active]
        elif self.historical_only:
            rows = [r for r in rows if not r.is_active]
        if self.limit:
            rows = rows[:self.limit]
        return rows

    def fetch_data(self) -> list:
        player_rows = self._get_players()
        loaded_ids  = self._get_loaded_player_ids()
        results     = []

        for i, player in enumerate(player_rows):
            if self._shutdown_requested:
                break

            player_id = player.id
            if player_id in loaded_ids:
                if VERBOSE:
                    print(f"  Skipping {player_id} (already loaded)")
                continue

            if VERBOSE:
                print(f"[{i+1}/{len(player_rows)}] Fetching player {player_id}...")

            response = self.api_call(
                commonplayerinfo.CommonPlayerInfo,
                player_id=player_id
            )

            if response is None:
                continue

            try:
                info    = response.common_player_info.get_dict()
                headers = info["headers"]
                data    = info["data"]
                if data:
                    row = dict(zip(headers, data[0]))
                    results.append({k: self._clean_value(v) for k, v in row.items()})
            except Exception as e:
                if VERBOSE:
                    print(f"  ⚠️  Could not parse player {player_id}: {e}")

            if (i + 1) % COOLDOWN_INTERVAL == 0:
                if VERBOSE:
                    print(f"  Cooling down for {COOLDOWN_TIME}s...")
                time.sleep(COOLDOWN_TIME)

        return results


def load_player_info(active_only=False, historical_only=False, limit=None, resume=False):
    PlayerInfoLoader(active_only=active_only, historical_only=historical_only, limit=limit, resume=resume).run()
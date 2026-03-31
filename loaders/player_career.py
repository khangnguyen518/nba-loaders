import time
from nba_api.stats.endpoints import playercareerstats
from loaders.base import BaseLoader
from config import COOLDOWN_INTERVAL, COOLDOWN_TIME, VERBOSE
from google.cloud import bigquery
from google.oauth2 import service_account
from config import BQ_PROJECT, BQ_DATASET, BQ_KEYFILE


class PlayerCareerLoader(BaseLoader):

    def __init__(self, active_only=False, historical_only=False, limit=None, resume=False):
        super().__init__()
        self.table_name      = "raw_player_career_stats"
        self.active_only     = active_only
        self.historical_only = historical_only
        self.limit           = limit
        self.resume          = resume
        self.write_mode      = 'upsert'
        self.upsert_keys     = ['PLAYER_ID', 'SEASON_ID', 'TEAM_ID']

    def get_create_table_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS `{self.dataset}.{self.table_name}` (
            PLAYER_ID         INT64,
            SEASON_ID         STRING,
            LEAGUE_ID         STRING,
            TEAM_ID           INT64,
            TEAM_ABBREVIATION STRING,
            PLAYER_AGE        FLOAT64,
            GP                INT64,
            GS                INT64,
            MIN               FLOAT64,
            FGM               INT64,
            FGA               INT64,
            FG_PCT            FLOAT64,
            FG3M              INT64,
            FG3A              INT64,
            FG3_PCT           FLOAT64,
            FTM               INT64,
            FTA               INT64,
            FT_PCT            FLOAT64,
            OREB              INT64,
            DREB              INT64,
            REB               INT64,
            AST               INT64,
            STL               INT64,
            BLK               INT64,
            TOV               INT64,
            PF                INT64,
            PTS               INT64,
            loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """

    def _get_loaded_player_ids(self) -> set:
        if not self.resume:
            return set()
        try:
            credentials = service_account.Credentials.from_service_account_file(BQ_KEYFILE)
            client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)
            query  = f"SELECT DISTINCT PLAYER_ID FROM `{BQ_PROJECT}.{BQ_DATASET}.{self.table_name}`"
            result = client.query(query).result()
            return {row.PLAYER_ID for row in result}
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
                print(f"[{i+1}/{len(player_rows)}] Fetching career stats for {player_id}...")

            response = self.api_call(
                playercareerstats.PlayerCareerStats,
                player_id=player_id,
                per_mode36="Totals"
            )

            if response is None:
                continue

            try:
                stats   = response.season_totals_regular_season.get_dict()
                headers = stats["headers"]
                for row_data in stats["data"]:
                    row = dict(zip(headers, row_data))
                    results.append({k: self._clean_value(v) for k, v in row.items()})
            except Exception as e:
                if VERBOSE:
                    print(f"  ⚠️  Could not parse player {player_id}: {e}")

            if (i + 1) % COOLDOWN_INTERVAL == 0:
                if VERBOSE:
                    print(f"  Cooling down for {COOLDOWN_TIME}s...")
                time.sleep(COOLDOWN_TIME)

        return results


def load_player_career(active_only=False, historical_only=False, limit=None, resume=False):
    PlayerCareerLoader(active_only=active_only, historical_only=historical_only, limit=limit, resume=resume).run()
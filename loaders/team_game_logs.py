from nba_api.stats.endpoints import leaguegamelog
from loaders.base import BaseLoader
from config import START_SEASON, END_SEASON, VERBOSE


class TeamGameLogsLoader(BaseLoader):

    def __init__(self, start_season=None, end_season=None):
        super().__init__()
        self.table_name   = "raw_team_game_logs"
        self.start_season = start_season if start_season is not None else START_SEASON
        self.end_season   = end_season   if end_season   is not None else END_SEASON
        self.write_mode   = 'upsert'
        self.upsert_keys  = ['TEAM_ID', 'GAME_ID']

    def get_create_table_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS `{self.dataset}.{self.table_name}` (
            SEASON_ID         STRING,
            TEAM_ID           INT64,
            TEAM_ABBREVIATION STRING,
            TEAM_NAME         STRING,
            GAME_ID           STRING,
            GAME_DATE         STRING,
            MATCHUP           STRING,
            WL                STRING,
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
            PLUS_MINUS        INT64,
            VIDEO_AVAILABLE   INT64,
            loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """

    def fetch_data(self) -> list:
        results = []

        for year in range(self.start_season, self.end_season + 1):
            if self._shutdown_requested:
                break

            season_id = f"{year}-{str(year + 1)[-2:]}"
            if VERBOSE:
                print(f"  Fetching team logs for {season_id}...")

            response = self.api_call(
                leaguegamelog.LeagueGameLog,
                season=season_id,
                player_or_team_abbreviation="T"
            )

            if response is None:
                continue

            try:
                logs    = response.league_game_log.get_dict()
                headers = logs["headers"]
                for row_data in logs["data"]:
                    row = dict(zip(headers, row_data))
                    row["SEASON_ID"] = season_id
                    results.append({k: self._clean_value(v) for k, v in row.items()})
            except Exception as e:
                if VERBOSE:
                    print(f"  ⚠️  Could not parse season {season_id}: {e}")

        return results


def load_team_game_logs(start_season=None, end_season=None):
    TeamGameLogsLoader(start_season=start_season, end_season=end_season).run()
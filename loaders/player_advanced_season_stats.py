from nba_api.stats.endpoints import leaguedashplayerstats
from loaders.base import BaseLoader
from config import VERBOSE

CURRENT_SEASON = "2025-26"

KEEP_COLUMNS = {
    "PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "TEAM_ABBREVIATION", "AGE",
    "GP", "W", "L", "MIN",
    "PER", "USG_PCT", "PACE", "PIE",
    "BPM", "OBPM", "DBPM", "VORP",
    "OFF_RATING", "DEF_RATING", "NET_RATING",
    "AST_PCT", "AST_TO", "REB_PCT", "OREB_PCT", "DREB_PCT",
}


class PlayerAdvancedSeasonStatsLoader(BaseLoader):

    def __init__(self, season: str = CURRENT_SEASON):
        super().__init__()
        self.table_name  = "raw_player_advanced_season_stats"
        self.season      = season
        self.write_mode  = "upsert"
        self.upsert_keys = ["PLAYER_ID", "SEASON_ID", "TEAM_ID"]

    def get_create_table_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS `{self.dataset}.{self.table_name}` (
            PLAYER_ID         INT64,
            PLAYER_NAME       STRING,
            TEAM_ID           INT64,
            TEAM_ABBREVIATION STRING,
            AGE               FLOAT64,
            GP                INT64,
            W                 INT64,
            L                 INT64,
            MIN               FLOAT64,
            PER               FLOAT64,
            USG_PCT           FLOAT64,
            PACE              FLOAT64,
            PIE               FLOAT64,
            BPM               FLOAT64,
            OBPM              FLOAT64,
            DBPM              FLOAT64,
            VORP              FLOAT64,
            OFF_RATING        FLOAT64,
            DEF_RATING        FLOAT64,
            NET_RATING        FLOAT64,
            AST_PCT           FLOAT64,
            AST_TO            FLOAT64,
            REB_PCT           FLOAT64,
            OREB_PCT          FLOAT64,
            DREB_PCT          FLOAT64,
            SEASON_ID         STRING,
            loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """

    def fetch_data(self) -> list:
        if VERBOSE:
            print(f"  Fetching advanced stats for season {self.season}...")

        response = self.api_call(
            leaguedashplayerstats.LeagueDashPlayerStats,
            season=self.season,
            measure_type_detailed_defense="Advanced",
            per_mode_simple="PerGame",
        )

        if response is None:
            return []

        try:
            data    = response.league_dash_player_stats.get_dict()
            headers = data["headers"]
            results = []
            for row_data in data["data"]:
                row = dict(zip(headers, row_data))
                filtered = {k: self._clean_value(v) for k, v in row.items() if k in KEEP_COLUMNS}
                filtered["SEASON_ID"] = self.season
                results.append(filtered)

            if VERBOSE:
                print(f"  ✓ Parsed {len(results)} player rows for {self.season}")
            return results

        except Exception as e:
            if VERBOSE:
                print(f"  ⚠️  Could not parse advanced stats for {self.season}: {e}")
            return []


def load_player_advanced_season_stats(season: str = CURRENT_SEASON):
    """Entry point for orchestration.

    Default run fetches the current season only ("2025-26").
    Pass a different `season` string (e.g. "2024-25") for one-off backfills.
    """
    PlayerAdvancedSeasonStatsLoader(season=season).run()
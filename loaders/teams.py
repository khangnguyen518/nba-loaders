from nba_api.stats.static import teams
from loaders.base import BaseLoader


class TeamsLoader(BaseLoader):

    def __init__(self):
        super().__init__()
        self.table_name = "raw_teams"
        self.write_mode = 'truncate'

    def get_create_table_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS `{self.dataset}.{self.table_name}` (
            id            INT64,
            full_name     STRING,
            abbreviation  STRING,
            nickname      STRING,
            city          STRING,
            state         STRING,
            year_founded  INT64,
            loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """

    def fetch_data(self) -> list:
        data = self.api_call(teams.get_teams)
        return data or []


def load_teams():
    TeamsLoader().run()
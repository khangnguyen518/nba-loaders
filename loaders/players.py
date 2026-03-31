from nba_api.stats.static import players
from loaders.base import BaseLoader


class PlayersLoader(BaseLoader):

    def __init__(self):
        super().__init__()
        self.table_name = "raw_players"
        self.write_mode = 'truncate'

    def get_create_table_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS `{self.dataset}.{self.table_name}` (
            id          INT64,
            full_name   STRING,
            first_name  STRING,
            last_name   STRING,
            is_active   BOOL,
            loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """

    def fetch_data(self) -> list:
        data = self.api_call(players.get_players)
        return data or []


def load_players():
    PlayersLoader().run()
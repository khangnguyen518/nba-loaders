import argparse
from db import test_connection
from loaders import (
    load_teams,
    load_players,
    load_player_info,
    load_player_career,
    load_game_logs,
    load_team_game_logs,
)


def main():
    parser = argparse.ArgumentParser(description="NBA raw data loader")

    parser.add_argument("--skip-teams",         action="store_true")
    parser.add_argument("--skip-players",        action="store_true")
    parser.add_argument("--skip-player-info",    action="store_true")
    parser.add_argument("--skip-player-career",  action="store_true")
    parser.add_argument("--skip-game-logs",      action="store_true")
    parser.add_argument("--skip-team-logs",      action="store_true")
    parser.add_argument("--active-only",         action="store_true",
                        help="Only process active players (~550)")
    parser.add_argument("--historical-only",      action="store_true",
                        help="Only process historical/inactive players (~4,550)")
    parser.add_argument("--limit-players",       type=int, default=None,
                        help="Process only N players (for testing)")
    parser.add_argument("--resume",              action="store_true",
                        help="Skip already-loaded players")
    parser.add_argument("--start-season",        type=int, default=2023)
    parser.add_argument("--end-season",          type=int, default=2025)

    args = parser.parse_args()

    if args.active_only and args.historical_only:
        print("❌ --active-only and --historical-only cannot be used together")
        return

    print("\n" + "="*60)
    print("          NBA RAW DATA LOADER")
    print("="*60)

    print("\nTesting BigQuery connection...")
    if not test_connection():
        print("❌ Could not connect to BigQuery. Check your .env file.")
        return

    if not args.skip_teams:
        load_teams()

    if not args.skip_players:
        load_players()

    if not args.skip_player_info:
        load_player_info(
            active_only=args.active_only,
            historical_only=args.historical_only,
            limit=args.limit_players,
            resume=args.resume
        )

    if not args.skip_player_career:
        load_player_career(
            active_only=args.active_only,
            historical_only=args.historical_only,
            limit=args.limit_players,
            resume=args.resume
        )

    if not args.skip_game_logs:
        load_game_logs(
            active_only=args.active_only,
            historical_only=args.historical_only,
            limit=args.limit_players,
            resume=args.resume,
            start_season=args.start_season,
            end_season=args.end_season
        )

    if not args.skip_team_logs:
        load_team_game_logs(
            start_season=args.start_season,
            end_season=args.end_season
        )

    print("\n" + "="*60)
    print("✓ All tables loaded successfully")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
import argparse
from datetime import datetime
from loaders import load_player_career, load_game_logs, load_team_game_logs


def get_current_season() -> int:
    now = datetime.now()
    return now.year - 1 if now.month < 10 else now.year


def main():
    parser = argparse.ArgumentParser(description="Daily NBA data update")

    parser.add_argument("--skip-career", action="store_true",
                        help="Skip career stats update, only load game logs")
    parser.add_argument("--full",        action="store_true",
                        help="Load full history instead of current season only")
    parser.add_argument("--start-year",  type=int, default=None)
    parser.add_argument("--end-year",    type=int, default=None)

    args = parser.parse_args()

    current_season = get_current_season()

    if args.full:
        start = args.start_year or current_season
        end   = args.end_year   or current_season
    else:
        start = current_season
        end   = current_season

    print("\n" + "="*60)
    print("          NBA DAILY UPDATE")
    print(f"          Season: {start}-{str(start + 1)[-2:]}")
    print("="*60)

    if not args.skip_career:
        print("\nUpdating career stats for active players...")
        load_player_career(active_only=True, resume=False)

    print("\nFetching new game logs...")
    load_game_logs(
        active_only=True,
        resume=False,
        start_season=start,
        end_season=end,
        current_season_only=True
    )

    print("\nFetching team game logs...")
    load_team_game_logs(start_season=start, end_season=end)

    print("\n" + "="*60)
    print("✓ Daily update complete")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
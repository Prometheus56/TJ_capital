#!/usr/bin/env python3
"""
Main CLI for the TVL pipeline.

Commands:
  setup   - Parse raw CSVs, create cleaned CSVs and recreate DB tables
  update  - Download daily data, transform and upsert rows into existing tables
"""
import argparse
from pathlib import Path
import sys
from dotenv import load_dotenv
import orchestrate
from orchestrate import main as transform_main
from load import Chains
load_dotenv()


def setup(raw_dir: Path, updated_dir: Path, tables: list[str] | None) -> None:
    # Override directories in orchestrate module
    orchestrate.RAW_DIR = raw_dir
    orchestrate.CLEAN_DIR = updated_dir
    orchestrate.CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    # Transform raw CSVs to cleaned CSVs
    print(f"🔄 Transforming raw CSVs in {raw_dir} to cleaned CSVs in {updated_dir}...")
    transform_main(tables)

    # Create or recreate tables in Postgres
    print(f"🔄 Creating tables from cleaned CSVs in {updated_dir}...")
    chains = Chains()
    chains.run_create_table(updated_dir)
    print("✅ Setup complete.")


def update() -> None:
    # Fetch today's data and upsert into DB
    print("🔄 Fetching today's data and updating tables...")
    chains = Chains()
    chains.add_row()
    print("✅ Daily update complete.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CLI for DefiLlama TVL pipeline"
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    # Setup command
    sp_setup = subparsers.add_parser('setup', help='Parse raw CSVs, clean and recreate tables')
    sp_setup.add_argument(
        '--raw-dir', type=Path,
        default=Path(r"/home/jakub/mnt/vpn_files/TJ_Capital/TJ_Capital_database"),
        help='Directory containing raw CSV files'
    )
    sp_setup.add_argument(
        '--clean-dir', type=Path,
        default=None,
        help='Directory to write cleaned CSVs (default: <raw-dir>/updated)'
    )
    sp_setup.add_argument(
        '--tables', nargs='*',
        help='Specific table names to process (default: all)'
    )

    # Update command
    subparsers.add_parser('update', help='Download daily data and update DB tables')

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.command == 'setup':
        raw_dir = args.raw_dir
        clean_dir = args.clean_dir or (raw_dir / 'updated')
        setup(raw_dir, clean_dir, args.tables)
    elif args.command == 'update':
        update()
    else:
        print("Unknown command. Use --help for usage.", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':  # pragma: no cover
    main()

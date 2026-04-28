from __future__ import annotations

import subprocess
import sys
import argparse
import os
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"


def run(script_path: Path, extra_args: list[str] | None = None) -> None:
    command = [sys.executable, str(script_path)]
    if extra_args:
        command.extend(extra_args)
    print(f"Running {script_path.name}...")
    subprocess.run(command, check=True)


def cleanup_source_files() -> None:
    source_files = [
        DATA_DIR / "customers.csv",
        DATA_DIR / "products.csv",
        DATA_DIR / "stores.csv",
        DATA_DIR / "transactions.csv",
        DATA_DIR / "sales_items.csv",
    ]
    for source_file in source_files:
        source_file.unlink(missing_ok=True)
    print("Cleaned generated source CSV files after database load.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the retail pipeline end-to-end.")
    parser.add_argument("--years", type=int, default=None, help="Generate a rolling range ending today.")
    parser.add_argument("--transactions", type=int, default=None, help="Number of synthetic transactions.")
    parser.add_argument(
        "--cleanup-source-files",
        action="store_true",
        help="Delete generated source CSV files after SQLite and DuckDB are created.",
    )
    args = parser.parse_args()

    generate_args: list[str] = []
    if args.years is not None:
        generate_args.extend(["--years", str(args.years)])
    if args.transactions is not None:
        generate_args.extend(["--transactions", str(args.transactions)])

    run(ROOT_DIR / "python" / "generate_data.py", generate_args)
    run(ROOT_DIR / "python" / "setup_sqlite.py")
    run(ROOT_DIR / "python" / "setup_duckdb.py")
    if args.cleanup_source_files:
        cleanup_source_files()
    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()

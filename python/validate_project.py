from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import duckdb


ROOT_DIR = Path(__file__).resolve().parent.parent
DBT_DIR = ROOT_DIR / "dbt_retail_analytics"
DB_PATH = ROOT_DIR / "data" / "retail_analytics.duckdb"
ML_OUTPUT_DIR = ROOT_DIR / "data" / "ml_outputs"


def run_command(command: list[str], cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
    working_dir = cwd if cwd is not None else ROOT_DIR
    printable = " ".join(command)
    print(f"\n>> Running: {printable}")
    subprocess.run(command, cwd=str(working_dir), env=env, check=True)


def validate_database() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB file not found: {DB_PATH}")

    connection = duckdb.connect(str(DB_PATH), read_only=True)
    row_count = connection.execute("SELECT COUNT(*) FROM fct_sales").fetchone()[0]
    connection.close()

    if row_count <= 0:
        raise ValueError("fct_sales has no rows.")

    print(f"OK: fct_sales row count = {row_count}")


def build_dbt_profile_file() -> Path:
    profile_dir = ROOT_DIR / ".tmp_dbt_profiles"
    profile_dir.mkdir(parents=True, exist_ok=True)
    profile_file = profile_dir / "profiles.yml"

    db_path = DB_PATH.resolve().as_posix()
    profile_content = (
        "retail_analytics:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: duckdb\n"
        f"      path: {db_path}\n"
        "      threads: 4\n"
    )

    profile_file.write_text(profile_content, encoding="utf-8")
    return profile_dir


def run_dbt() -> None:
    profile_dir = build_dbt_profile_file()
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(profile_dir)

    dbt_executable_name = "dbt.exe" if os.name == "nt" else "dbt"
    dbt_executable = Path(sys.executable).parent / dbt_executable_name
    dbt_command = str(dbt_executable) if dbt_executable.exists() else "dbt"

    run_command([dbt_command, "run"], cwd=DBT_DIR, env=env)
    run_command([dbt_command, "test"], cwd=DBT_DIR, env=env)


def validate_ml_outputs() -> None:
    expected = {
        "model_metrics.json",
        "regression_feature_importance.csv",
        "cluster_assignments.csv",
        "pca_projections.csv",
    }

    missing = [name for name in expected if not (ML_OUTPUT_DIR / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing ML outputs: {missing}")

    print(f"OK: ML outputs generated in {ML_OUTPUT_DIR}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate project pipeline end-to-end.")
    parser.add_argument(
        "--include-dbt",
        action="store_true",
        help="Run dbt run and dbt test as part of validation.",
    )
    args = parser.parse_args()

    run_command([sys.executable, "python/run_pipeline.py"])
    validate_database()

    if args.include_dbt:
        run_dbt()

    run_command([sys.executable, "python/ml_retail.py"])
    validate_ml_outputs()

    run_command([sys.executable, "-m", "py_compile", "streamlit/app.py"])
    print("\nSUCCESS: Project validation completed.")


if __name__ == "__main__":
    main()

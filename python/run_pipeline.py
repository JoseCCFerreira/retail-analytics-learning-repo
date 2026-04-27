from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent


def run(script_path: Path) -> None:
    print(f"Running {script_path.name}...")
    subprocess.run([sys.executable, str(script_path)], check=True)


def main() -> None:
    run(ROOT_DIR / "python" / "generate_data.py")
    run(ROOT_DIR / "python" / "setup_sqlite.py")
    run(ROOT_DIR / "python" / "setup_duckdb.py")
    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()

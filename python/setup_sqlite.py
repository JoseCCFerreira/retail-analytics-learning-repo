from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
SQLITE_SQL_DIR = ROOT_DIR / "sql" / "sqlite"
SQLITE_DB_PATH = DATA_DIR / "retail_app.db"


def run_sql_script(connection: sqlite3.Connection, script_path: Path) -> None:
    sql = script_path.read_text(encoding="utf-8")
    connection.executescript(sql)


def load_csv_to_table(connection: sqlite3.Connection, table_name: str, csv_path: Path) -> None:
    dataframe = pd.read_csv(csv_path)
    dataframe.to_sql(table_name, connection, if_exists="append", index=False)


def main() -> None:
    if not DATA_DIR.exists():
        raise FileNotFoundError("Missing data directory. Run python/generate_data.py first.")

    with sqlite3.connect(SQLITE_DB_PATH) as connection:
        connection.execute("PRAGMA foreign_keys = ON;")

        run_sql_script(connection, SQLITE_SQL_DIR / "01_create_tables.sql")

        load_csv_to_table(connection, "customers", DATA_DIR / "customers.csv")
        load_csv_to_table(connection, "products", DATA_DIR / "products.csv")
        load_csv_to_table(connection, "stores", DATA_DIR / "stores.csv")
        load_csv_to_table(connection, "transactions", DATA_DIR / "transactions.csv")
        load_csv_to_table(connection, "sales_items", DATA_DIR / "sales_items.csv")

        run_sql_script(connection, SQLITE_SQL_DIR / "02_indexes.sql")

        connection.commit()

    print(f"SQLite database created: {SQLITE_DB_PATH}")


if __name__ == "__main__":
    main()

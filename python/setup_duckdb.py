from __future__ import annotations

from pathlib import Path

import duckdb


ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
DUCKDB_DB_PATH = DATA_DIR / "retail_analytics.duckdb"


def main() -> None:
    if not DATA_DIR.exists():
        raise FileNotFoundError("Missing data directory. Run python/generate_data.py first.")

    connection = duckdb.connect(str(DUCKDB_DB_PATH))

    connection.execute("DROP TABLE IF EXISTS customers")
    connection.execute("DROP TABLE IF EXISTS products")
    connection.execute("DROP TABLE IF EXISTS stores")
    connection.execute("DROP TABLE IF EXISTS transactions")
    connection.execute("DROP TABLE IF EXISTS sales_items")
    connection.execute("DROP TABLE IF EXISTS fct_sales")

    connection.execute(f"CREATE TABLE customers AS SELECT * FROM read_csv_auto('{(DATA_DIR / 'customers.csv').as_posix()}')")
    connection.execute(f"CREATE TABLE products AS SELECT * FROM read_csv_auto('{(DATA_DIR / 'products.csv').as_posix()}')")
    connection.execute(f"CREATE TABLE stores AS SELECT * FROM read_csv_auto('{(DATA_DIR / 'stores.csv').as_posix()}')")
    connection.execute(f"CREATE TABLE transactions AS SELECT * FROM read_csv_auto('{(DATA_DIR / 'transactions.csv').as_posix()}')")
    connection.execute(f"CREATE TABLE sales_items AS SELECT * FROM read_csv_auto('{(DATA_DIR / 'sales_items.csv').as_posix()}')")

    connection.execute(
        """
        CREATE TABLE fct_sales AS
        SELECT
            si.sales_item_id,
            t.transaction_id,
            CAST(t.transaction_date AS DATE) AS transaction_date,
            t.customer_id,
            t.store_id,
            t.payment_method,
            si.product_id,
            si.quantity,
            si.unit_price,
            si.discount_pct,
            si.gross_amount,
            si.discount_amount,
            si.net_amount
        FROM sales_items si
        INNER JOIN transactions t
            ON t.transaction_id = si.transaction_id
        """
    )

    connection.close()
    print(f"DuckDB database created: {DUCKDB_DB_PATH}")


if __name__ == "__main__":
    main()

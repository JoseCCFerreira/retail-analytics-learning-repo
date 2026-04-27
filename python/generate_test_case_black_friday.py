from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import random

import duckdb
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parent.parent
CASE_DIR = ROOT_DIR / "data" / "test_cases" / "black_friday_2025"
CASE_DB = CASE_DIR / "retail_case_black_friday.duckdb"
SEED = 20251128


def build_customers(total: int = 160) -> pd.DataFrame:
    rows: list[dict] = []
    segments = ["budget", "family", "premium", "business"]
    cities = ["Lisboa", "Porto", "Braga", "Coimbra", "Faro"]
    for customer_id in range(1, total + 1):
        rows.append(
            {
                "customer_id": customer_id,
                "customer_name": f"CaseCustomer {customer_id:04d}",
                "segment": random.choice(segments),
                "city": random.choice(cities),
            }
        )
    return pd.DataFrame(rows)


def build_products() -> pd.DataFrame:
    categories = ["beverage", "bakery", "dairy", "frozen", "snacks", "cleaning"]
    rows: list[dict] = []
    product_id = 1
    for category in categories:
        for _ in range(10):
            base_price = {
                "beverage": (1.0, 8.0),
                "bakery": (0.8, 7.0),
                "dairy": (1.2, 12.0),
                "frozen": (2.0, 18.0),
                "snacks": (0.9, 10.0),
                "cleaning": (2.5, 20.0),
            }[category]
            rows.append(
                {
                    "product_id": product_id,
                    "product_name": f"CaseProduct {product_id:03d}",
                    "category": category,
                    "unit_price": round(random.uniform(*base_price), 2),
                }
            )
            product_id += 1
    return pd.DataFrame(rows)


def build_stores(total: int = 8) -> pd.DataFrame:
    regions = ["north", "center", "south"]
    rows = []
    for store_id in range(1, total + 1):
        rows.append(
            {
                "store_id": store_id,
                "store_name": f"CaseStore {store_id:02d}",
                "region": random.choice(regions),
            }
        )
    return pd.DataFrame(rows)


def daterange(start: date, end: date) -> list[date]:
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def generate_case_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    random.seed(SEED)

    customers_df = build_customers()
    products_df = build_products()
    stores_df = build_stores()

    product_records = products_df.to_dict("records")
    customer_ids = customers_df["customer_id"].tolist()
    store_ids = stores_df["store_id"].tolist()

    start_date = date(2025, 11, 1)
    end_date = date(2025, 12, 15)

    transactions_rows: list[dict] = []
    sales_rows: list[dict] = []
    transaction_id = 1
    sales_item_id = 1

    for tx_date in daterange(start_date, end_date):
        # Baseline daily traffic
        daily_transactions = random.randint(35, 55)

        # Black Friday surge + Cyber Monday aftershock
        if tx_date == date(2025, 11, 28):
            daily_transactions = random.randint(160, 220)
        elif tx_date in {date(2025, 11, 29), date(2025, 11, 30), date(2025, 12, 1)}:
            daily_transactions = random.randint(95, 140)

        # Logistics disruption week: volume up but more discount pressure
        if date(2025, 12, 5) <= tx_date <= date(2025, 12, 10):
            daily_transactions = int(daily_transactions * 0.9)

        for _ in range(daily_transactions):
            payment_weights = [0.18, 0.62, 0.20]  # cash, card, mbway
            if tx_date >= date(2025, 11, 27):
                payment_weights = [0.09, 0.58, 0.33]  # mobile payment adoption rises

            payment_method = random.choices(["cash", "card", "mbway"], weights=payment_weights, k=1)[0]

            transactions_rows.append(
                {
                    "transaction_id": transaction_id,
                    "transaction_date": tx_date.isoformat(),
                    "customer_id": random.choice(customer_ids),
                    "store_id": random.choice(store_ids),
                    "payment_method": payment_method,
                }
            )

            items_per_tx = random.randint(1, 5)
            selected = random.sample(product_records, k=items_per_tx)

            for product in selected:
                quantity = random.randint(1, 4)
                base_unit_price = float(product["unit_price"])
                category = str(product["category"])

                # Event mix shift: snacks and cleaning are promoted strongly
                if tx_date == date(2025, 11, 28) and category in {"snacks", "cleaning"}:
                    discount_pct = random.choice([15, 20, 25, 30])
                elif tx_date in {date(2025, 11, 29), date(2025, 11, 30), date(2025, 12, 1)}:
                    discount_pct = random.choice([5, 10, 15, 20])
                elif date(2025, 12, 5) <= tx_date <= date(2025, 12, 10):
                    discount_pct = random.choice([10, 15, 20])
                else:
                    discount_pct = random.choice([0, 0, 0, 5, 10])

                gross_amount = round(quantity * base_unit_price, 2)
                discount_amount = round(gross_amount * (discount_pct / 100), 2)
                net_amount = round(gross_amount - discount_amount, 2)

                sales_rows.append(
                    {
                        "sales_item_id": sales_item_id,
                        "transaction_id": transaction_id,
                        "product_id": int(product["product_id"]),
                        "quantity": quantity,
                        "unit_price": base_unit_price,
                        "discount_pct": discount_pct,
                        "gross_amount": gross_amount,
                        "discount_amount": discount_amount,
                        "net_amount": net_amount,
                    }
                )
                sales_item_id += 1

            transaction_id += 1

    transactions_df = pd.DataFrame(transactions_rows)
    sales_items_df = pd.DataFrame(sales_rows)
    return customers_df, products_df, stores_df, transactions_df, sales_items_df


def write_case_files(
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    stores_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    sales_items_df: pd.DataFrame,
) -> None:
    CASE_DIR.mkdir(parents=True, exist_ok=True)

    customers_df.to_csv(CASE_DIR / "customers_case.csv", index=False)
    products_df.to_csv(CASE_DIR / "products_case.csv", index=False)
    stores_df.to_csv(CASE_DIR / "stores_case.csv", index=False)
    transactions_df.to_csv(CASE_DIR / "transactions_case.csv", index=False)
    sales_items_df.to_csv(CASE_DIR / "sales_items_case.csv", index=False)

    expected_rows = [
        {
            "signal": "bf_revenue_spike",
            "target_window": "2025-11-28 vs baseline 2025-11-01..2025-11-20",
            "expected_direction": "up",
            "why": "Black Friday promotions and traffic surge",
        },
        {
            "signal": "discount_ratio_up",
            "target_window": "2025-11-28..2025-12-01",
            "expected_direction": "up",
            "why": "Aggressive campaign discounting",
        },
        {
            "signal": "mobile_payment_share_up",
            "target_window": "from 2025-11-27 onward",
            "expected_direction": "up",
            "why": "Shift from cash/card to mbway during campaign",
        },
        {
            "signal": "category_mix_shift",
            "target_window": "2025-11-28",
            "expected_direction": "snacks_and_cleaning_up",
            "why": "Promoted categories on Black Friday",
        },
    ]
    pd.DataFrame(expected_rows).to_csv(CASE_DIR / "expected_signals_case.csv", index=False)


def build_case_duckdb() -> None:
    connection = duckdb.connect(str(CASE_DB))

    connection.execute("DROP TABLE IF EXISTS customers")
    connection.execute("DROP TABLE IF EXISTS products")
    connection.execute("DROP TABLE IF EXISTS stores")
    connection.execute("DROP TABLE IF EXISTS transactions")
    connection.execute("DROP TABLE IF EXISTS sales_items")
    connection.execute("DROP TABLE IF EXISTS fct_sales")

    connection.execute(
        f"CREATE TABLE customers AS SELECT * FROM read_csv_auto('{(CASE_DIR / 'customers_case.csv').as_posix()}')"
    )
    connection.execute(
        f"CREATE TABLE products AS SELECT * FROM read_csv_auto('{(CASE_DIR / 'products_case.csv').as_posix()}')"
    )
    connection.execute(
        f"CREATE TABLE stores AS SELECT * FROM read_csv_auto('{(CASE_DIR / 'stores_case.csv').as_posix()}')"
    )
    connection.execute(
        f"CREATE TABLE transactions AS SELECT * FROM read_csv_auto('{(CASE_DIR / 'transactions_case.csv').as_posix()}')"
    )
    connection.execute(
        f"CREATE TABLE sales_items AS SELECT * FROM read_csv_auto('{(CASE_DIR / 'sales_items_case.csv').as_posix()}')"
    )

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


def main() -> None:
    customers_df, products_df, stores_df, transactions_df, sales_items_df = generate_case_data()
    write_case_files(customers_df, products_df, stores_df, transactions_df, sales_items_df)
    build_case_duckdb()

    print(f"Case dataset generated in: {CASE_DIR}")
    print(f"Case DuckDB generated: {CASE_DB}")


if __name__ == "__main__":
    main()

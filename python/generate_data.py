from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import random

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
SEED = 42


@dataclass
class GeneratorConfig:
    customers: int = 300
    products: int = 80
    stores: int = 12
    transactions: int = 2200
    max_items_per_transaction: int = 5
    start_date: date = date(2024, 1, 1)
    end_date: date = date(2025, 12, 31)


def _random_dates(start: date, end: date, count: int) -> list[date]:
    delta = (end - start).days
    return [start + timedelta(days=random.randint(0, delta)) for _ in range(count)]


def build_customers(total: int) -> pd.DataFrame:
    segments = ["budget", "family", "premium", "business"]
    rows = []
    for customer_id in range(1, total + 1):
        rows.append(
            {
                "customer_id": customer_id,
                "customer_name": f"Customer {customer_id:04d}",
                "segment": random.choice(segments),
                "city": random.choice(["Lisboa", "Porto", "Braga", "Coimbra", "Faro"]),
            }
        )
    return pd.DataFrame(rows)


def build_products(total: int) -> pd.DataFrame:
    categories = ["beverage", "bakery", "dairy", "frozen", "snacks", "cleaning"]
    rows = []
    for product_id in range(1, total + 1):
        rows.append(
            {
                "product_id": product_id,
                "product_name": f"Product {product_id:03d}",
                "category": random.choice(categories),
                "unit_price": round(random.uniform(0.9, 45.0), 2),
            }
        )
    return pd.DataFrame(rows)


def build_stores(total: int) -> pd.DataFrame:
    rows = []
    for store_id in range(1, total + 1):
        rows.append(
            {
                "store_id": store_id,
                "store_name": f"Store {store_id:02d}",
                "region": random.choice(["north", "center", "south"]),
            }
        )
    return pd.DataFrame(rows)


def build_transactions(
    total: int,
    customer_ids: list[int],
    store_ids: list[int],
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    transaction_dates = _random_dates(start_date, end_date, total)
    rows = []
    for transaction_id in range(1, total + 1):
        rows.append(
            {
                "transaction_id": transaction_id,
                "transaction_date": transaction_dates[transaction_id - 1].isoformat(),
                "customer_id": random.choice(customer_ids),
                "store_id": random.choice(store_ids),
                "payment_method": random.choice(["cash", "card", "mbway"]),
            }
        )
    return pd.DataFrame(rows)


def build_sales_items(transactions_df: pd.DataFrame, products_df: pd.DataFrame, max_items_per_tx: int) -> pd.DataFrame:
    rows = []
    sales_item_id = 1
    for transaction in transactions_df.itertuples(index=False):
        items_count = random.randint(1, max_items_per_tx)
        selected_products = random.sample(products_df.to_dict("records"), k=items_count)

        for product in selected_products:
            quantity = random.randint(1, 4)
            unit_price = float(product["unit_price"])
            discount_pct = random.choice([0, 0, 0, 5, 10, 15])
            gross_amount = round(quantity * unit_price, 2)
            discount_amount = round(gross_amount * (discount_pct / 100), 2)
            net_amount = round(gross_amount - discount_amount, 2)

            rows.append(
                {
                    "sales_item_id": sales_item_id,
                    "transaction_id": transaction.transaction_id,
                    "product_id": int(product["product_id"]),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount_pct": discount_pct,
                    "gross_amount": gross_amount,
                    "discount_amount": discount_amount,
                    "net_amount": net_amount,
                }
            )
            sales_item_id += 1

    return pd.DataFrame(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic retail source data.")
    parser.add_argument("--customers", type=int, default=GeneratorConfig.customers)
    parser.add_argument("--products", type=int, default=GeneratorConfig.products)
    parser.add_argument("--stores", type=int, default=GeneratorConfig.stores)
    parser.add_argument("--transactions", type=int, default=GeneratorConfig.transactions)
    parser.add_argument("--max-items-per-transaction", type=int, default=GeneratorConfig.max_items_per_transaction)
    parser.add_argument("--start-date", default=GeneratorConfig.start_date.isoformat())
    parser.add_argument("--end-date", default=GeneratorConfig.end_date.isoformat())
    parser.add_argument(
        "--years",
        type=int,
        default=None,
        help="Generate a rolling range ending today. Example: --years 2.",
    )
    parser.add_argument("--seed", type=int, default=SEED)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(args.seed)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    end_date = date.fromisoformat(args.end_date)
    start_date = date.fromisoformat(args.start_date)
    if args.years is not None:
        end_date = date.today()
        start_date = end_date - timedelta(days=365 * args.years)

    config = GeneratorConfig(
        customers=args.customers,
        products=args.products,
        stores=args.stores,
        transactions=args.transactions,
        max_items_per_transaction=args.max_items_per_transaction,
        start_date=start_date,
        end_date=end_date,
    )

    customers_df = build_customers(config.customers)
    products_df = build_products(config.products)
    stores_df = build_stores(config.stores)
    transactions_df = build_transactions(
        config.transactions,
        customer_ids=customers_df["customer_id"].tolist(),
        store_ids=stores_df["store_id"].tolist(),
        start_date=config.start_date,
        end_date=config.end_date,
    )
    sales_items_df = build_sales_items(
        transactions_df=transactions_df,
        products_df=products_df,
        max_items_per_tx=config.max_items_per_transaction,
    )

    customers_df.to_csv(DATA_DIR / "customers.csv", index=False)
    products_df.to_csv(DATA_DIR / "products.csv", index=False)
    stores_df.to_csv(DATA_DIR / "stores.csv", index=False)
    transactions_df.to_csv(DATA_DIR / "transactions.csv", index=False)
    sales_items_df.to_csv(DATA_DIR / "sales_items.csv", index=False)

    print(
        "CSV files generated in data/ "
        f"({config.transactions} transactions from {config.start_date} to {config.end_date})"
    )


if __name__ == "__main__":
    main()

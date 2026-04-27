from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler


ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "data" / "retail_analytics.duckdb"
OUTPUT_DIR = ROOT_DIR / "data" / "deep_learning_outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_retail_data() -> pd.DataFrame:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"DuckDB not found at {DB_PATH}. Run: python python/run_pipeline.py"
        )

    conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = conn.execute(
        """
        SELECT
            f.transaction_id,
            f.transaction_date,
            f.customer_id,
            f.store_id,
            f.product_id,
            f.quantity,
            f.unit_price,
            f.discount_pct,
            f.gross_amount,
            f.net_amount,
            f.payment_method,
            p.category,
            p.product_name,
            c.segment AS customer_segment,
            c.city AS customer_city,
            s.region AS store_region
        FROM fct_sales f
        JOIN products p ON p.product_id = f.product_id
        JOIN customers c ON c.customer_id = f.customer_id
        JOIN stores s ON s.store_id = f.store_id
        """
    ).df()
    conn.close()
    return df


def feature_engineering(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str], LabelEncoder]:
    df = df.copy()
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    df["day_of_week"] = df["transaction_date"].dt.dayofweek
    df["month"] = df["transaction_date"].dt.month
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["has_discount"] = (df["discount_pct"] > 0).astype(int)

    segment_encoder = LabelEncoder()
    df["segment_encoded"] = segment_encoder.fit_transform(df["customer_segment"])

    for col in ["category", "customer_city", "store_region", "payment_method"]:
        encoder = LabelEncoder()
        df[f"{col}_encoded"] = encoder.fit_transform(df[col].astype(str))

    feature_cols = [
        "quantity",
        "unit_price",
        "discount_pct",
        "has_discount",
        "day_of_week",
        "month",
        "is_weekend",
        "category_encoded",
        "customer_city_encoded",
        "store_region_encoded",
        "payment_method_encoded",
    ]
    return df, feature_cols, segment_encoder


def build_regression_dataset(
    test_size: float = 0.2, random_state: int = 42
) -> tuple[pd.DataFrame, list[str], StandardScaler, tuple]:
    df = load_retail_data()
    df, feature_cols, _ = feature_engineering(df)

    X = df[feature_cols].values
    y = df["net_amount"].values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    return df, feature_cols, scaler, (X_train_scaled, X_test_scaled, y_train, y_test)


def build_classification_dataset(
    test_size: float = 0.2, random_state: int = 42
) -> tuple[pd.DataFrame, list[str], LabelEncoder, StandardScaler, tuple]:
    df = load_retail_data()
    df, _, segment_encoder = feature_engineering(df)

    customer_df = df.groupby("customer_id").agg(
        total_spent=("net_amount", "sum"),
        avg_basket=("net_amount", "mean"),
        num_transactions=("transaction_id", "nunique"),
        avg_quantity=("quantity", "mean"),
        avg_discount=("discount_pct", "mean"),
        favourite_category=("category_encoded", "median"),
        preferred_payment=("payment_method_encoded", lambda x: x.mode()[0]),
        store_region=("store_region_encoded", "median"),
    ).reset_index()

    customer_df = customer_df.merge(
        df[["customer_id", "segment_encoded"]].drop_duplicates(),
        on="customer_id",
    )

    feature_cols = [
        "total_spent",
        "avg_basket",
        "num_transactions",
        "avg_quantity",
        "avg_discount",
        "favourite_category",
        "preferred_payment",
        "store_region",
    ]

    X = customer_df[feature_cols].values
    y = customer_df["segment_encoded"].values

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=test_size,
        random_state=random_state,
        stratify=y,
    )
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    return customer_df, feature_cols, segment_encoder, scaler, (
        X_train_scaled,
        X_test_scaled,
        y_train,
        y_test,
    )

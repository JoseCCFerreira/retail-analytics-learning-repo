"""
ml_retail.py
============
Retail Analytics Machine Learning Pipeline
-------------------------------------------
Runs Supervised + Unsupervised ML models over the retail DuckDB data.

Usage:
    python python/ml_retail.py

Outputs in data/ml_outputs/:
    - model metrics JSON
    - feature importance CSV
    - cluster assignments CSV
    - PCA projections CSV
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

# ── Scikit-learn imports ──────────────────────────────────────────────────────
from sklearn.cluster import DBSCAN, KMeans
from sklearn.decomposition import PCA
from sklearn.ensemble import GradientBoostingRegressor, RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    silhouette_score,
)
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor

warnings.filterwarnings("ignore")

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "data" / "retail_analytics.duckdb"
OUTPUT_DIR = ROOT_DIR / "data" / "ml_outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SEPARATOR = "=" * 60


# ──────────────────────────────────────────────────────────────────────────────
# 1. DATA LOADING
# ──────────────────────────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    """
    Load and join all relevant tables from DuckDB into a single flat DataFrame.

    Tables used:
        fct_sales       → fact table with transactions, amounts, discounts
        products        → product name, category, unit_price
        customers       → customer segment, city
        stores          → store region
    """
    print(f"\n{SEPARATOR}")
    print("📦  LOADING DATA FROM DuckDB")
    print(SEPARATOR)

    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"DuckDB not found at {DB_PATH}.\n"
            "Run: python python/run_pipeline.py  first."
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
            c.segment       AS customer_segment,
            c.city          AS customer_city,
            s.region        AS store_region
        FROM fct_sales f
        JOIN products p ON p.product_id = f.product_id
        JOIN customers    c ON c.customer_id = f.customer_id
        JOIN stores       s ON s.store_id    = f.store_id
        """
    ).df()
    conn.close()

    print(f"  ✅  Rows loaded : {len(df):,}")
    print(f"  ✅  Columns     : {list(df.columns)}")
    return df


# ──────────────────────────────────────────────────────────────────────────────
# 2. FEATURE ENGINEERING
# ──────────────────────────────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> tuple[pd.DataFrame, LabelEncoder]:
    """
    Build ML-ready features from raw columns.

    Encoding strategy:
        - Categorical columns → LabelEncoder (ordinal integers)
        - Boolean flags       → 0/1
        - Dates               → decomposed to day_of_week, month, is_weekend

    Returns (feature_df, label_encoder_for_segment)
    """
    print(f"\n{SEPARATOR}")
    print("🔧  FEATURE ENGINEERING")
    print(SEPARATOR)

    df = df.copy()
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    df["day_of_week"] = df["transaction_date"].dt.dayofweek          # 0=Mon
    df["month"] = df["transaction_date"].dt.month
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["has_discount"] = (df["discount_pct"] > 0).astype(int)

    le_segment = LabelEncoder()
    df["segment_encoded"] = le_segment.fit_transform(df["customer_segment"])

    for col in ["category", "customer_city", "store_region", "payment_method"]:
        le = LabelEncoder()
        df[f"{col}_encoded"] = le.fit_transform(df[col].astype(str))

    feature_cols = [
        "quantity", "unit_price", "discount_pct", "has_discount",
        "day_of_week", "month", "is_weekend",
        "category_encoded", "customer_city_encoded",
        "store_region_encoded", "payment_method_encoded",
    ]

    print(f"  ✅  Feature columns: {feature_cols}")
    return df, le_segment, feature_cols


# ──────────────────────────────────────────────────────────────────────────────
# 3. SUPERVISED LEARNING — REGRESSION
# ──────────────────────────────────────────────────────────────────────────────

def run_regression(df: pd.DataFrame, feature_cols: list[str]) -> dict:
    """
    GOAL: Predict net_amount (revenue per line item).

    Models tested:
        1. Linear Regression        — simple baseline, assumes linearity
        2. Decision Tree Regressor  — non-linear, interpretable splits
        3. Random Forest Regressor  — ensemble of trees, reduces variance
        4. Gradient Boosting        — boosted ensemble, best typical accuracy

    Metrics:
        MAE  = Mean Absolute Error    (average error in €)
        RMSE = Root Mean Squared Error (penalises large errors)
        R²   = coefficient of determination (1.0 = perfect)
    """
    print(f"\n{SEPARATOR}")
    print("📈  SUPERVISED LEARNING — REGRESSION (predict net_amount)")
    print(SEPARATOR)

    X = df[feature_cols].values
    y = df["net_amount"].values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    scaler = StandardScaler()
    X_train_sc = scaler.fit_transform(X_train)
    X_test_sc = scaler.transform(X_test)

    models = {
        "Linear Regression": LinearRegression(),
        "Decision Tree": DecisionTreeRegressor(max_depth=6, random_state=42),
        "Random Forest": RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1),
        "Gradient Boosting": GradientBoostingRegressor(n_estimators=100, random_state=42),
    }

    results = {}
    for name, model in models.items():
        use_scaled = name == "Linear Regression"
        Xtr = X_train_sc if use_scaled else X_train
        Xte = X_test_sc if use_scaled else X_test

        model.fit(Xtr, y_train)
        preds = model.predict(Xte)

        mae = mean_absolute_error(y_test, preds)
        rmse = np.sqrt(mean_squared_error(y_test, preds))
        r2 = r2_score(y_test, preds)

        results[name] = {"MAE": round(mae, 4), "RMSE": round(rmse, 4), "R2": round(r2, 4)}
        print(f"\n  🔹 {name}")
        print(f"     MAE  = {mae:.4f} €")
        print(f"     RMSE = {rmse:.4f} €")
        print(f"     R²   = {r2:.4f}")

    # Feature importance from Random Forest
    rf_model = models["Random Forest"]
    importances = pd.DataFrame({
        "feature": feature_cols,
        "importance": rf_model.feature_importances_,
    }).sort_values("importance", ascending=False)

    imp_path = OUTPUT_DIR / "regression_feature_importance.csv"
    importances.to_csv(imp_path, index=False)
    print(f"\n  💾  Feature importances saved → {imp_path}")

    return results


# ──────────────────────────────────────────────────────────────────────────────
# 4. SUPERVISED LEARNING — CLASSIFICATION
# ──────────────────────────────────────────────────────────────────────────────

def run_classification(df: pd.DataFrame, feature_cols: list[str], le_segment: LabelEncoder) -> dict:
    """
    GOAL: Predict customer_segment ('budget','family','premium','business').

    Models tested:
        1. Logistic Regression      — linear probabilistic classifier (baseline)
        2. Decision Tree Classifier — interpretable rule-based splits
        3. Random Forest Classifier — ensemble majority vote
    
    Metrics:
        Accuracy   — % of correct predictions overall
        Precision  — of all predicted class X, how many really are X?
        Recall     — of all real class X, how many did we catch?
        F1-Score   — harmonic mean of precision + recall
    """
    print(f"\n{SEPARATOR}")
    print("🏷️   SUPERVISED LEARNING — CLASSIFICATION (predict customer segment)")
    print(SEPARATOR)

    # Build customer-level aggregated features
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

    # Merge segment label
    seg_map = df[["customer_id", "segment_encoded"]].drop_duplicates()
    customer_df = customer_df.merge(seg_map, on="customer_id")

    feat_cols = [
        "total_spent", "avg_basket", "num_transactions",
        "avg_quantity", "avg_discount",
        "favourite_category", "preferred_payment", "store_region",
    ]

    X = customer_df[feat_cols].values
    y = customer_df["segment_encoded"].values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    scaler = StandardScaler()
    X_train_sc = scaler.fit_transform(X_train)
    X_test_sc = scaler.transform(X_test)

    models = {
        "Logistic Regression": LogisticRegression(max_iter=1000, random_state=42),
        "Decision Tree": DecisionTreeClassifier(max_depth=5, random_state=42),
        "Random Forest": RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1),
    }

    results = {}
    for name, model in models.items():
        use_scaled = name == "Logistic Regression"
        Xtr = X_train_sc if use_scaled else X_train
        Xte = X_test_sc if use_scaled else X_test

        model.fit(Xtr, y_train)
        preds = model.predict(Xte)

        acc = accuracy_score(y_test, preds)
        report = classification_report(
            y_test, preds,
            target_names=le_segment.classes_,
            output_dict=True,
            zero_division=0,
        )
        results[name] = {"Accuracy": round(acc, 4), "report": report}

        print(f"\n  🔹 {name}")
        print(f"     Accuracy = {acc:.4f}")
        print(
            classification_report(
                y_test, preds,
                target_names=le_segment.classes_,
                zero_division=0,
            )
        )

    return results


# ──────────────────────────────────────────────────────────────────────────────
# 5. UNSUPERVISED LEARNING — CLUSTERING
# ──────────────────────────────────────────────────────────────────────────────

def run_clustering(df: pd.DataFrame) -> pd.DataFrame:
    """
    GOAL: Discover natural customer groups without using predefined segments.

    Models tested:
        1. K-Means   — centroid-based, needs k upfront, fast, assumes spherical clusters
        2. DBSCAN    — density-based, finds arbitrary shapes, auto-detects outliers (-1 label)

    Evaluation:
        Silhouette Score — measures how similar a point is to its own cluster
                           vs neighbouring clusters. Range [-1, 1], higher = better.
        -1 means noise / outlier in DBSCAN.
    """
    print(f"\n{SEPARATOR}")
    print("🔵  UNSUPERVISED LEARNING — CLUSTERING (customer segmentation)")
    print(SEPARATOR)

    # Aggregate to customer level
    customer_df = df.groupby("customer_id").agg(
        total_spent=("net_amount", "sum"),
        avg_basket=("net_amount", "mean"),
        num_transactions=("transaction_id", "nunique"),
        avg_quantity=("quantity", "mean"),
        avg_discount=("discount_pct", "mean"),
    ).reset_index()

    features = ["total_spent", "avg_basket", "num_transactions", "avg_quantity", "avg_discount"]
    X = customer_df[features].values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # ── K-Means ──────────────────────────────────────────────────────────────
    print("\n  🔹 K-Means (k=4 clusters)")
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    kmeans_labels = kmeans.fit_predict(X_scaled)
    sil_km = silhouette_score(X_scaled, kmeans_labels)
    customer_df["kmeans_cluster"] = kmeans_labels
    print(f"     Silhouette Score = {sil_km:.4f}")
    print(f"     Cluster sizes:\n{pd.Series(kmeans_labels).value_counts().sort_index().to_string()}")

    # Find optimal k by elbow (k=2..8)
    inertias = {}
    for k in range(2, 9):
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        km.fit(X_scaled)
        inertias[k] = round(km.inertia_, 2)
    print(f"\n     Elbow inertias (lower = tighter clusters):")
    for k, inertia in inertias.items():
        print(f"       k={k}: {inertia}")

    # ── DBSCAN ───────────────────────────────────────────────────────────────
    print("\n  🔹 DBSCAN (eps=0.8, min_samples=5)")
    dbscan = DBSCAN(eps=0.8, min_samples=5)
    dbscan_labels = dbscan.fit_predict(X_scaled)
    customer_df["dbscan_cluster"] = dbscan_labels
    n_clusters = len(set(dbscan_labels)) - (1 if -1 in dbscan_labels else 0)
    n_noise = (dbscan_labels == -1).sum()
    print(f"     Clusters found : {n_clusters}")
    print(f"     Noise points   : {n_noise}")
    if n_clusters > 1:
        mask = dbscan_labels != -1
        sil_db = silhouette_score(X_scaled[mask], dbscan_labels[mask])
        print(f"     Silhouette Score (excl. noise) = {sil_db:.4f}")

    # Save
    out_path = OUTPUT_DIR / "cluster_assignments.csv"
    customer_df.to_csv(out_path, index=False)
    print(f"\n  💾  Cluster assignments saved → {out_path}")
    return customer_df


# ──────────────────────────────────────────────────────────────────────────────
# 6. UNSUPERVISED LEARNING — PCA (Dimensionality Reduction)
# ──────────────────────────────────────────────────────────────────────────────

def run_pca(cluster_df: pd.DataFrame) -> None:
    """
    GOAL: Reduce 5 customer features to 2 principal components for visualisation.

    Theory:
        PCA finds the directions of maximum variance in high-dimensional data.
        PC1 captures the most variance; PC2 captures the second most.
        Together, they let us plot clusters on a 2-D scatter — impossible in 5-D.

    Explained variance ratio:
        How much of the original information is retained in each component.
        e.g. PC1=45%, PC2=30% → 2 components explain 75% of variance.
    """
    print(f"\n{SEPARATOR}")
    print("🌐  UNSUPERVISED LEARNING — PCA (dimensionality reduction)")
    print(SEPARATOR)

    features = ["total_spent", "avg_basket", "num_transactions", "avg_quantity", "avg_discount"]
    X = cluster_df[features].values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    pca = PCA(n_components=2, random_state=42)
    components = pca.fit_transform(X_scaled)

    pca_df = pd.DataFrame({
        "pc1": components[:, 0],
        "pc2": components[:, 1],
        "kmeans_cluster": cluster_df["kmeans_cluster"].values,
        "dbscan_cluster": cluster_df["dbscan_cluster"].values,
    })

    ev = pca.explained_variance_ratio_
    print(f"  PC1 explains {ev[0]*100:.1f}% of variance")
    print(f"  PC2 explains {ev[1]*100:.1f}% of variance")
    print(f"  Total explained variance: {sum(ev)*100:.1f}%")

    out_path = OUTPUT_DIR / "pca_projections.csv"
    pca_df.to_csv(out_path, index=False)
    print(f"\n  💾  PCA projections saved → {out_path}")

    # Full PCA to see all components
    pca_full = PCA(random_state=42)
    pca_full.fit(X_scaled)
    cumulative = np.cumsum(pca_full.explained_variance_ratio_)
    print(f"\n  Cumulative explained variance per component:")
    for i, c in enumerate(cumulative, 1):
        print(f"    {i} components → {c*100:.1f}%")


# ──────────────────────────────────────────────────────────────────────────────
# 7. CROSS-VALIDATION
# ──────────────────────────────────────────────────────────────────────────────

def run_cross_validation(df: pd.DataFrame, feature_cols: list[str]) -> None:
    """
    5-fold cross-validation on Random Forest Regressor.

    Why cross-validate?
        A single train/test split can be lucky/unlucky.
        CV trains + evaluates on 5 different splits and reports mean ± std.
        More honest estimate of real-world performance.
    """
    print(f"\n{SEPARATOR}")
    print("🔄  CROSS-VALIDATION (5-fold, Random Forest Regressor)")
    print(SEPARATOR)

    X = df[feature_cols].values
    y = df["net_amount"].values

    rf = RandomForestRegressor(n_estimators=50, random_state=42, n_jobs=-1)
    scores = cross_val_score(rf, X, y, cv=5, scoring="r2")

    print(f"  R² scores per fold: {[round(s, 4) for s in scores]}")
    print(f"  Mean R² : {scores.mean():.4f}")
    print(f"  Std  R² : {scores.std():.4f}")
    print(
        "\n  Interpretation:\n"
        "    • Mean close to 1.0  → model explains most variance\n"
        "    • Low std            → stable across different data splits\n"
        "    • Mean < 0           → model worse than predicting the mean"
    )


# ──────────────────────────────────────────────────────────────────────────────
# 8. SAVE METRICS SUMMARY
# ──────────────────────────────────────────────────────────────────────────────

def save_metrics(regression_results: dict, classification_results: dict) -> None:
    summary = {
        "regression": regression_results,
        "classification": {
            name: {"Accuracy": v["Accuracy"]}
            for name, v in classification_results.items()
        },
    }
    out_path = OUTPUT_DIR / "model_metrics.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    print(f"\n{SEPARATOR}")
    print(f"📊  METRICS SUMMARY saved → {out_path}")
    print(SEPARATOR)
    print(json.dumps(summary, indent=2))


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"\n{'#'*60}")
    print("   RETAIL ANALYTICS — MACHINE LEARNING PIPELINE")
    print(f"{'#'*60}")

    df = load_data()
    df, le_segment, feature_cols = engineer_features(df)

    reg_results = run_regression(df, feature_cols)
    cls_results = run_classification(df, feature_cols, le_segment)
    cluster_df = run_clustering(df)
    run_pca(cluster_df)
    run_cross_validation(df, feature_cols)
    save_metrics(reg_results, cls_results)

    print(f"\n{'#'*60}")
    print("   ✅  ML PIPELINE COMPLETE")
    print(f"   Outputs in: {OUTPUT_DIR}")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    main()

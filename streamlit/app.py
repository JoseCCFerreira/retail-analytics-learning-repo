from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st


ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "data" / "retail_analytics.duckdb"
ML_OUTPUT_DIR = ROOT_DIR / "data" / "ml_outputs"


@st.cache_data
def load_kpis() -> dict[str, float]:
    connection = duckdb.connect(str(DB_PATH), read_only=True)
    result = connection.execute(
        """
        SELECT
            COUNT(DISTINCT transaction_id) AS total_transactions,
            ROUND(SUM(net_amount), 2) AS total_revenue,
            ROUND(AVG(net_amount), 2) AS avg_line_amount
        FROM fct_sales
        """
    ).fetchone()
    connection.close()

    return {
        "total_transactions": result[0],
        "total_revenue": result[1],
        "avg_line_amount": result[2],
    }


@st.cache_data
def load_monthly_revenue() -> pd.DataFrame:
    connection = duckdb.connect(str(DB_PATH), read_only=True)
    dataframe = connection.execute(
        """
        SELECT
            date_trunc('month', transaction_date) AS month_date,
            ROUND(SUM(net_amount), 2) AS revenue
        FROM fct_sales
        GROUP BY 1
        ORDER BY 1
        """
    ).df()
    connection.close()
    return dataframe


@st.cache_data
def load_top_products(limit: int = 10) -> pd.DataFrame:
    connection = duckdb.connect(str(DB_PATH), read_only=True)
    dataframe = connection.execute(
        """
        SELECT
            p.product_name,
            p.category,
            ROUND(SUM(f.net_amount), 2) AS revenue,
            SUM(f.quantity) AS units_sold
        FROM fct_sales f
        JOIN products p ON p.product_id = f.product_id
        GROUP BY p.product_name, p.category
        ORDER BY revenue DESC
        LIMIT ?
        """,
        [limit],
    ).df()
    connection.close()
    return dataframe


@st.cache_data
def load_category_revenue() -> pd.DataFrame:
    connection = duckdb.connect(str(DB_PATH), read_only=True)
    dataframe = connection.execute(
        """
        SELECT
            p.category,
            ROUND(SUM(f.net_amount), 2) AS revenue,
            SUM(f.quantity) AS units_sold
        FROM fct_sales f
        JOIN products p ON p.product_id = f.product_id
        GROUP BY p.category
        ORDER BY revenue DESC
        """
    ).df()
    connection.close()
    return dataframe


def build_retail_metric_frames(metrics: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    regression_rows = []
    for model_name, values in metrics.get("regression", {}).items():
        regression_rows.append(
            {
                "model": model_name,
                "MAE": values.get("MAE"),
                "RMSE": values.get("RMSE"),
                "R2": values.get("R2"),
            }
        )

    classification_rows = []
    for model_name, values in metrics.get("classification", {}).items():
        classification_rows.append(
            {
                "model": model_name,
                "Accuracy": values.get("Accuracy"),
            }
        )

    return pd.DataFrame(regression_rows), pd.DataFrame(classification_rows)


@st.cache_data
def load_ml_outputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    feature_path = ML_OUTPUT_DIR / "regression_feature_importance.csv"
    pca_path = ML_OUTPUT_DIR / "pca_projections.csv"
    metrics_path = ML_OUTPUT_DIR / "model_metrics.json"
    feature_df = pd.read_csv(feature_path) if feature_path.exists() else pd.DataFrame()
    pca_df = pd.read_csv(pca_path) if pca_path.exists() else pd.DataFrame()
    metrics = json.loads(metrics_path.read_text(encoding="utf-8")) if metrics_path.exists() else {}
    regression_df, classification_df = build_retail_metric_frames(metrics)
    return feature_df, pca_df, regression_df, classification_df


def main() -> None:
    st.set_page_config(page_title="Retail Analytics Dashboard", layout="wide")
    st.title("Retail Analytics Dashboard")

    if not DB_PATH.exists():
        st.warning("Base DuckDB não encontrada. Executa: python python/run_pipeline.py")
        return

    kpis = load_kpis()

    col1, col2, col3 = st.columns(3)
    col1.metric("Transações", f"{kpis['total_transactions']:,}")
    col2.metric("Receita Total", f"€ {kpis['total_revenue']:,.2f}")
    col3.metric("Valor Médio por Linha", f"€ {kpis['avg_line_amount']:,.2f}")

    monthly_revenue = load_monthly_revenue()
    fig_month = px.line(
        monthly_revenue,
        x="month_date",
        y="revenue",
        markers=True,
        title="Receita por Mês",
    )
    st.plotly_chart(fig_month, use_container_width=True)

    top_products = load_top_products(limit=10)
    fig_products = px.bar(
        top_products,
        x="product_name",
        y="revenue",
        color="category",
        title="Top 10 Produtos por Receita",
    )
    st.plotly_chart(fig_products, use_container_width=True)

    st.subheader("Detalhe Top Produtos")
    st.dataframe(top_products, use_container_width=True)

    category_revenue = load_category_revenue()
    fig_category = px.bar(
        category_revenue,
        x="category",
        y="revenue",
        color="category",
        title="Receita por Categoria",
    )
    st.plotly_chart(fig_category, use_container_width=True)

    feature_importance, pca, regression_metrics, classification_metrics = load_ml_outputs()
    if not feature_importance.empty or not pca.empty or not regression_metrics.empty or not classification_metrics.empty:
        st.subheader("Machine Learning: entender, aplicar e analisar")
    if not regression_metrics.empty:
        col_reg, col_cls = st.columns(2)
        with col_reg:
            fig_r2 = px.bar(
                regression_metrics.sort_values("R2", ascending=False),
                x="model",
                y="R2",
                title="Comparação de Modelos de Regressão por R2",
            )
            st.plotly_chart(fig_r2, use_container_width=True)
            st.dataframe(regression_metrics, use_container_width=True)
        if not classification_metrics.empty:
            with col_cls:
                fig_acc = px.bar(
                    classification_metrics.sort_values("Accuracy", ascending=False),
                    x="model",
                    y="Accuracy",
                    title="Comparação de Modelos de Classificação por Accuracy",
                )
                st.plotly_chart(fig_acc, use_container_width=True)
                st.dataframe(classification_metrics, use_container_width=True)
    if not feature_importance.empty:
        fig_importance = px.bar(
            feature_importance.head(12),
            x="importance",
            y="feature",
            orientation="h",
            title="Importância das Features na Regressão",
        )
        st.plotly_chart(fig_importance, use_container_width=True)
    if not pca.empty:
        fig_pca = px.scatter(
            pca,
            x="pc1",
            y="pc2",
            color=pca["kmeans_cluster"].astype(str),
            title="Clusters de Clientes em PCA",
            labels={"color": "kmeans_cluster"},
        )
        st.plotly_chart(fig_pca, use_container_width=True)


if __name__ == "__main__":
    main()

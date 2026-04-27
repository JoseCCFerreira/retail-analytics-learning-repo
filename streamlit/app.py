from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st


ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "data" / "retail_analytics.duckdb"


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


if __name__ == "__main__":
    main()

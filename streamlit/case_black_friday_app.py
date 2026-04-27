from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st


ROOT_DIR = Path(__file__).resolve().parent.parent
CASE_DB_PATH = ROOT_DIR / "data" / "test_cases" / "black_friday_2025" / "retail_case_black_friday.duckdb"
BASELINE_START = "2025-11-01"
BASELINE_END = "2025-11-20"
CAMPAIGN_START = "2025-11-28"
CAMPAIGN_END = "2025-12-01"


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(CASE_DB_PATH), read_only=True)


@st.cache_data
def load_daily_revenue() -> pd.DataFrame:
    con = get_connection()
    df = con.execute(
        """
        SELECT
            transaction_date,
            ROUND(SUM(net_amount), 2) AS revenue,
            ROUND(SUM(discount_amount), 2) AS discount_amount,
            ROUND(100.0 * SUM(discount_amount) / NULLIF(SUM(gross_amount), 0), 2) AS discount_ratio_pct
        FROM fct_sales
        GROUP BY 1
        ORDER BY 1
        """
    ).df()
    con.close()
    return df


@st.cache_data
def load_payment_mix() -> pd.DataFrame:
    con = get_connection()
    df = con.execute(
        """
        WITH base AS (
            SELECT
                CASE
                    WHEN transaction_date BETWEEN DATE ? AND DATE ? THEN 'baseline'
                    WHEN transaction_date BETWEEN DATE ? AND DATE ? THEN 'campaign'
                    ELSE 'other'
                END AS period,
                payment_method,
                COUNT(DISTINCT transaction_id) AS tx_count
            FROM fct_sales
            GROUP BY 1, 2
        ),
        totals AS (
            SELECT period, SUM(tx_count) AS total_tx
            FROM base
            WHERE period IN ('baseline', 'campaign')
            GROUP BY period
        )
        SELECT
            b.period,
            b.payment_method,
            b.tx_count,
            ROUND(100.0 * b.tx_count / NULLIF(t.total_tx, 0), 2) AS share_pct
        FROM base b
        JOIN totals t ON t.period = b.period
        WHERE b.period IN ('baseline', 'campaign')
        ORDER BY b.period, share_pct DESC
        """,
        [BASELINE_START, BASELINE_END, CAMPAIGN_START, CAMPAIGN_END],
    ).df()
    con.close()
    return df


@st.cache_data
def load_category_mix() -> pd.DataFrame:
    con = get_connection()
    df = con.execute(
        """
        WITH base AS (
            SELECT
                p.category,
                CASE
                    WHEN f.transaction_date BETWEEN DATE ? AND DATE ? THEN 'baseline'
                    WHEN f.transaction_date BETWEEN DATE ? AND DATE ? THEN 'campaign'
                    ELSE 'other'
                END AS period,
                SUM(f.net_amount) AS revenue
            FROM fct_sales f
            JOIN products p ON p.product_id = f.product_id
            GROUP BY 1, 2
        )
        SELECT
            category,
            period,
            ROUND(revenue, 2) AS revenue
        FROM base
        WHERE period IN ('baseline', 'campaign')
        ORDER BY category, period
        """,
        [BASELINE_START, BASELINE_END, CAMPAIGN_START, CAMPAIGN_END],
    ).df()
    con.close()
    return df


@st.cache_data
def load_kpis() -> dict[str, float]:
    con = get_connection()
    result = con.execute(
        """
        WITH day_rev AS (
            SELECT
                transaction_date,
                SUM(net_amount) AS revenue,
                SUM(discount_amount) AS discount_amount,
                SUM(gross_amount) AS gross_amount
            FROM fct_sales
            GROUP BY 1
        ),
        window_stats AS (
            SELECT
                AVG(CASE WHEN transaction_date BETWEEN DATE ? AND DATE ? THEN revenue END) AS baseline_daily_avg,
                AVG(CASE WHEN transaction_date BETWEEN DATE ? AND DATE ? THEN revenue END) AS campaign_daily_avg,
                AVG(CASE WHEN transaction_date BETWEEN DATE ? AND DATE ? THEN discount_amount / NULLIF(gross_amount, 0) END) AS baseline_discount_ratio,
                AVG(CASE WHEN transaction_date BETWEEN DATE ? AND DATE ? THEN discount_amount / NULLIF(gross_amount, 0) END) AS campaign_discount_ratio
            FROM day_rev
        ),
        payment AS (
            SELECT
                SUM(CASE WHEN transaction_date BETWEEN DATE ? AND DATE ? AND payment_method = 'mbway' THEN 1 ELSE 0 END) AS baseline_mbway_tx,
                SUM(CASE WHEN transaction_date BETWEEN DATE ? AND DATE ? THEN 1 ELSE 0 END) AS baseline_total_tx,
                SUM(CASE WHEN transaction_date BETWEEN DATE ? AND DATE ? AND payment_method = 'mbway' THEN 1 ELSE 0 END) AS campaign_mbway_tx,
                SUM(CASE WHEN transaction_date BETWEEN DATE ? AND DATE ? THEN 1 ELSE 0 END) AS campaign_total_tx
            FROM transactions
        )
        SELECT
            ROUND(w.baseline_daily_avg, 2) AS baseline_daily_avg,
            ROUND(w.campaign_daily_avg, 2) AS campaign_daily_avg,
            ROUND((w.campaign_daily_avg / NULLIF(w.baseline_daily_avg, 0) - 1) * 100, 2) AS campaign_uplift_pct,
            ROUND(w.baseline_discount_ratio * 100, 2) AS baseline_discount_ratio_pct,
            ROUND(w.campaign_discount_ratio * 100, 2) AS campaign_discount_ratio_pct,
            ROUND(100.0 * p.baseline_mbway_tx / NULLIF(p.baseline_total_tx, 0), 2) AS baseline_mbway_share_pct,
            ROUND(100.0 * p.campaign_mbway_tx / NULLIF(p.campaign_total_tx, 0), 2) AS campaign_mbway_share_pct
        FROM window_stats w
        CROSS JOIN payment p
        """,
        [
            BASELINE_START,
            BASELINE_END,
            CAMPAIGN_START,
            CAMPAIGN_END,
            BASELINE_START,
            BASELINE_END,
            CAMPAIGN_START,
            CAMPAIGN_END,
            BASELINE_START,
            BASELINE_END,
            BASELINE_START,
            BASELINE_END,
            CAMPAIGN_START,
            CAMPAIGN_END,
            CAMPAIGN_START,
            CAMPAIGN_END,
        ],
    ).fetchone()
    con.close()

    return {
        "baseline_daily_avg": result[0],
        "campaign_daily_avg": result[1],
        "campaign_uplift_pct": result[2],
        "baseline_discount_ratio_pct": result[3],
        "campaign_discount_ratio_pct": result[4],
        "baseline_mbway_share_pct": result[5],
        "campaign_mbway_share_pct": result[6],
    }


def main() -> None:
    st.set_page_config(page_title="Black Friday Case Dashboard", layout="wide")
    st.title("Black Friday 2025 Case Dashboard")
    st.caption("Baseline vs Campaign behavior analysis for retail dynamics")

    if not CASE_DB_PATH.exists():
        st.warning("Case DB not found. Run: python python/generate_test_case_black_friday.py")
        return

    kpis = load_kpis()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Baseline Daily Avg Revenue", f"EUR {kpis['baseline_daily_avg']:,.2f}")
    col2.metric("Campaign Daily Avg Revenue", f"EUR {kpis['campaign_daily_avg']:,.2f}")
    col3.metric("Campaign Uplift", f"{kpis['campaign_uplift_pct']:,.2f}%")
    col4.metric(
        "MBWay Share Shift",
        f"{kpis['campaign_mbway_share_pct']:,.2f}%",
        delta=f"vs baseline {kpis['baseline_mbway_share_pct']:,.2f}%",
    )

    c1, c2 = st.columns(2)
    c1.metric("Baseline Discount Ratio", f"{kpis['baseline_discount_ratio_pct']:,.2f}%")
    c2.metric("Campaign Discount Ratio", f"{kpis['campaign_discount_ratio_pct']:,.2f}%")

    st.subheader("Daily Revenue and Discount Pressure")
    daily_df = load_daily_revenue()
    fig_line = px.line(
        daily_df,
        x="transaction_date",
        y=["revenue", "discount_ratio_pct"],
        markers=True,
        title="Revenue and Discount Ratio by Day",
    )
    st.plotly_chart(fig_line, use_container_width=True)

    st.subheader("Payment Mix: Baseline vs Campaign")
    payment_df = load_payment_mix()
    fig_pay = px.bar(
        payment_df,
        x="payment_method",
        y="share_pct",
        color="period",
        barmode="group",
        title="Payment Method Share",
    )
    st.plotly_chart(fig_pay, use_container_width=True)

    st.subheader("Category Revenue Mix: Baseline vs Campaign")
    category_df = load_category_mix()
    fig_cat = px.bar(
        category_df,
        x="category",
        y="revenue",
        color="period",
        barmode="group",
        title="Category Mix Shift",
    )
    st.plotly_chart(fig_cat, use_container_width=True)

    with st.expander("Show raw comparison tables"):
        st.markdown("**Payment mix table**")
        st.dataframe(payment_df, use_container_width=True)
        st.markdown("**Category mix table**")
        st.dataframe(category_df, use_container_width=True)


if __name__ == "__main__":
    main()

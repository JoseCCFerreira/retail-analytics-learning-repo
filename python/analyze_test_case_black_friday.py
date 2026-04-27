from __future__ import annotations

from pathlib import Path

import duckdb


ROOT_DIR = Path(__file__).resolve().parent.parent
CASE_DB = ROOT_DIR / "data" / "test_cases" / "black_friday_2025" / "retail_case_black_friday.duckdb"


def main() -> None:
    if not CASE_DB.exists():
        raise FileNotFoundError(
            f"Case DB not found: {CASE_DB}. Run python python/generate_test_case_black_friday.py first."
        )

    con = duckdb.connect(str(CASE_DB), read_only=True)

    print("\n=== BLACK FRIDAY TEST CASE ANALYSIS ===")

    baseline = con.execute(
        """
        SELECT ROUND(SUM(net_amount), 2) AS baseline_revenue
        FROM fct_sales
        WHERE transaction_date BETWEEN DATE '2025-11-01' AND DATE '2025-11-20'
        """
    ).fetchone()[0]

    bf_day = con.execute(
        """
        SELECT ROUND(SUM(net_amount), 2) AS bf_revenue
        FROM fct_sales
        WHERE transaction_date = DATE '2025-11-28'
        """
    ).fetchone()[0]

    baseline_daily = round(float(baseline) / 20.0, 2)
    uplift_pct = round(((float(bf_day) / baseline_daily) - 1.0) * 100.0, 2)

    print(f"Baseline revenue (2025-11-01..20): {baseline}")
    print(f"Baseline daily avg: {baseline_daily}")
    print(f"Black Friday revenue (2025-11-28): {bf_day}")
    print(f"Black Friday uplift vs baseline daily avg: {uplift_pct}%")

    discount_ratio = con.execute(
        """
        WITH daily AS (
            SELECT
                transaction_date,
                SUM(discount_amount) AS discount_total,
                SUM(gross_amount) AS gross_total
            FROM fct_sales
            GROUP BY 1
        )
        SELECT
            ROUND(100 * AVG(CASE WHEN transaction_date BETWEEN DATE '2025-11-01' AND DATE '2025-11-20'
                                 THEN discount_total / NULLIF(gross_total, 0)
                            END), 2) AS baseline_discount_pct,
            ROUND(100 * AVG(CASE WHEN transaction_date BETWEEN DATE '2025-11-28' AND DATE '2025-12-01'
                                 THEN discount_total / NULLIF(gross_total, 0)
                            END), 2) AS campaign_discount_pct
        FROM daily
        """
    ).fetchone()

    print(f"Discount ratio baseline (%): {discount_ratio[0]}")
    print(f"Discount ratio campaign (%): {discount_ratio[1]}")

    payment_mix = con.execute(
        """
        WITH mix AS (
            SELECT
                CASE
                    WHEN transaction_date BETWEEN DATE '2025-11-01' AND DATE '2025-11-20' THEN 'baseline'
                    WHEN transaction_date >= DATE '2025-11-27' THEN 'campaign_plus'
                    ELSE 'other'
                END AS period,
                payment_method,
                COUNT(DISTINCT transaction_id) AS tx_count
            FROM fct_sales
            GROUP BY 1, 2
        ),
        totals AS (
            SELECT period, SUM(tx_count) AS total_tx
            FROM mix
            WHERE period IN ('baseline', 'campaign_plus')
            GROUP BY period
        )
        SELECT
            m.period,
            m.payment_method,
            m.tx_count,
            ROUND(100.0 * m.tx_count / NULLIF(t.total_tx, 0), 2) AS share_pct
        FROM mix m
        JOIN totals t ON t.period = m.period
        WHERE m.period IN ('baseline', 'campaign_plus')
        ORDER BY m.period, share_pct DESC
        """
    ).fetchall()

    print("\nPayment mix comparison (baseline vs campaign_plus):")
    for row in payment_mix:
        print(f"  period={row[0]:13s} payment={row[1]:6s} tx={row[2]:5d} share={row[3]:6.2f}%")

    category_mix = con.execute(
        """
        SELECT
            p.category,
            ROUND(SUM(CASE WHEN f.transaction_date = DATE '2025-11-28' THEN f.net_amount ELSE 0 END), 2) AS bf_revenue,
            ROUND(SUM(CASE WHEN f.transaction_date BETWEEN DATE '2025-11-01' AND DATE '2025-11-20' THEN f.net_amount ELSE 0 END), 2) AS baseline_revenue
        FROM fct_sales f
        JOIN products p ON p.product_id = f.product_id
        GROUP BY 1
        ORDER BY bf_revenue DESC
        """
    ).fetchall()

    print("\nCategory mix on Black Friday vs baseline (absolute revenue):")
    for row in category_mix:
        print(f"  category={row[0]:9s} bf={row[1]:10.2f} baseline_total={row[2]:12.2f}")

    con.close()


if __name__ == "__main__":
    main()

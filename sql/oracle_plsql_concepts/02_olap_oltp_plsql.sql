-- =============================================================================
-- OLAP vs OLTP + PL/SQL Advanced Study Case
-- Retail Analytics — Oracle Concepts (Conceptual / Educational)
-- =============================================================================
-- Este ficheiro explora os conceitos de OLTP vs OLAP, funções analíticas
-- avançadas, e padrões de PL/SQL para processamento em batch.
-- =============================================================================


-- =============================================================================
-- SECTION 1: OLTP vs OLAP — O QUE SÃO?
-- =============================================================================
/*
  OLTP (Online Transaction Processing)
  ─────────────────────────────────────
  • Optimizado para INSERT / UPDATE / DELETE de alta frequência
  • Tabelas normalizadas (3NF) para evitar redundância
  • Muitos utilizadores concorrentes, operações curtas
  • Exemplo: sistema POS de caixa de supermercado
  • Tabelas típicas: transactions, sales_items, customers, stores, products

  OLAP (Online Analytical Processing)
  ─────────────────────────────────────
  • Optimizado para SELECT complexo sobre grandes volumes
  • Tabelas desnormalizadas (Star Schema / Snowflake Schema)
  • Poucos utilizadores analíticos, queries longas mas pesadas
  • Exemplo: Data Warehouse para relatórios mensais de direcção
  • Tabelas típicas: fct_sales, dim_products, dim_customers, dim_date

  HTAP (Hybrid Transactional/Analytical Processing)
  ──────────────────────────────────────────────────
  • Combina OLTP + OLAP no mesmo sistema (ex: DuckDB, Snowflake, BigQuery)
  • Permite análise em real-time sem ETL batch

  Star Schema (o que usamos neste projecto):
  ──────────────────────────────────────────
                    ┌──────────────┐
                    │  dim_date    │
                    └──────┬───────┘
  ┌────────────┐    ┌──────┴───────┐    ┌──────────────┐
  │ dim_customers├──┤  fct_sales   ├──┤ dim_products  │
  └────────────┘    └──────┬───────┘    └──────────────┘
                    ┌──────┴───────┐
                    │  dim_stores  │
                    └──────────────┘
*/


-- =============================================================================
-- SECTION 2: OLTP TABLES — Normalized Design (3NF)
-- =============================================================================

-- In OLTP we keep data normalised to avoid update anomalies.
-- This is what SQLite holds in this project.

CREATE TABLE IF NOT EXISTS oltp_customers (
    customer_id   INTEGER PRIMARY KEY,
    customer_name VARCHAR2(100) NOT NULL,
    segment       VARCHAR2(20)  NOT NULL CHECK (segment IN ('budget','family','premium','business')),
    city          VARCHAR2(50)  NOT NULL,
    created_at    DATE DEFAULT SYSDATE
);

CREATE TABLE IF NOT EXISTS oltp_transactions (
    transaction_id   INTEGER PRIMARY KEY,
    transaction_date DATE    NOT NULL,
    customer_id      INTEGER NOT NULL REFERENCES oltp_customers(customer_id),
    store_id         INTEGER NOT NULL,
    payment_method   VARCHAR2(10) CHECK (payment_method IN ('cash','card','mbway'))
);

CREATE TABLE IF NOT EXISTS oltp_sales_items (
    sales_item_id  INTEGER PRIMARY KEY,
    transaction_id INTEGER NOT NULL REFERENCES oltp_transactions(transaction_id),
    product_id     INTEGER NOT NULL,
    quantity       INTEGER NOT NULL CHECK (quantity > 0),
    unit_price     NUMBER(10,2) NOT NULL CHECK (unit_price > 0),
    discount_pct   NUMBER(5,2)  DEFAULT 0 CHECK (discount_pct BETWEEN 0 AND 100),
    gross_amount   NUMBER(12,2) GENERATED ALWAYS AS (quantity * unit_price) VIRTUAL,
    net_amount     NUMBER(12,2) -- calculated after discount
);


-- =============================================================================
-- SECTION 3: OLAP TABLES — Star Schema (Denormalized)
-- =============================================================================

-- Dimension tables: descriptive attributes
CREATE TABLE IF NOT EXISTS dim_date (
    date_id       INTEGER PRIMARY KEY,  -- YYYYMMDD
    full_date     DATE NOT NULL,
    day_of_week   INTEGER,              -- 1=Sunday ... 7=Saturday
    day_name      VARCHAR2(10),
    month_num     INTEGER,
    month_name    VARCHAR2(10),
    quarter       INTEGER,
    year_num      INTEGER,
    is_weekend    CHAR(1) DEFAULT 'N',
    is_holiday    CHAR(1) DEFAULT 'N'
);

-- Fact table: measurements + foreign keys to all dimensions
CREATE TABLE IF NOT EXISTS fct_sales_olap (
    sales_key      INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_id        INTEGER REFERENCES dim_date(date_id),
    customer_id    INTEGER,
    store_id       INTEGER,
    product_id     INTEGER,
    transaction_id INTEGER,
    quantity       INTEGER,
    unit_price     NUMBER(10,2),
    discount_pct   NUMBER(5,2),
    gross_amount   NUMBER(12,2),
    net_amount     NUMBER(12,2),
    payment_method VARCHAR2(10)
);

-- Indexes for analytical queries (bitmap indexes in real Oracle DW)
CREATE INDEX IF NOT EXISTS idx_fct_date     ON fct_sales_olap(date_id);
CREATE INDEX IF NOT EXISTS idx_fct_customer ON fct_sales_olap(customer_id);
CREATE INDEX IF NOT EXISTS idx_fct_store    ON fct_sales_olap(store_id);
CREATE INDEX IF NOT EXISTS idx_fct_product  ON fct_sales_olap(product_id);


-- =============================================================================
-- SECTION 4: ETL — OLTP → OLAP (PL/SQL Batch Procedure)
-- =============================================================================
/*
  ETL = Extract, Transform, Load
  ──────────────────────────────
  • Extract  → read from OLTP normalized tables
  • Transform → compute aggregations, join dimensions, clean data
  • Load     → insert into OLAP fact/dimension tables

  This is the classic nightly batch job that populates your Data Warehouse.
*/

CREATE OR REPLACE PROCEDURE prc_etl_oltp_to_olap (
    p_load_date IN DATE DEFAULT TRUNC(SYSDATE)
) AS
    v_rows_loaded  INTEGER := 0;
    v_date_id      INTEGER;
BEGIN
    -- Step 1: Populate dim_date if needed
    v_date_id := TO_NUMBER(TO_CHAR(p_load_date, 'YYYYMMDD'));

    MERGE INTO dim_date tgt
    USING (
        SELECT
            TO_NUMBER(TO_CHAR(p_load_date, 'YYYYMMDD'))  AS date_id,
            TRUNC(p_load_date)                            AS full_date,
            TO_NUMBER(TO_CHAR(p_load_date, 'D'))          AS day_of_week,
            TO_CHAR(p_load_date, 'Day')                   AS day_name,
            TO_NUMBER(TO_CHAR(p_load_date, 'MM'))         AS month_num,
            TO_CHAR(p_load_date, 'Month')                 AS month_name,
            TO_NUMBER(TO_CHAR(p_load_date, 'Q'))          AS quarter,
            TO_NUMBER(TO_CHAR(p_load_date, 'YYYY'))       AS year_num,
            CASE WHEN TO_CHAR(p_load_date,'D') IN ('1','7') THEN 'Y' ELSE 'N' END AS is_weekend
        FROM DUAL
    ) src
    ON (tgt.date_id = src.date_id)
    WHEN NOT MATCHED THEN
        INSERT (date_id, full_date, day_of_week, day_name, month_num,
                month_name, quarter, year_num, is_weekend)
        VALUES (src.date_id, src.full_date, src.day_of_week, src.day_name,
                src.month_num, src.month_name, src.quarter, src.year_num, src.is_weekend);

    -- Step 2: Load fact table from OLTP source
    INSERT INTO fct_sales_olap (
        date_id, customer_id, store_id, product_id, transaction_id,
        quantity, unit_price, discount_pct, gross_amount, net_amount, payment_method
    )
    SELECT
        v_date_id,
        t.customer_id,
        t.store_id,
        si.product_id,
        t.transaction_id,
        si.quantity,
        si.unit_price,
        si.discount_pct,
        si.quantity * si.unit_price                                     AS gross_amount,
        si.quantity * si.unit_price * (1 - si.discount_pct / 100.0)    AS net_amount,
        t.payment_method
    FROM oltp_transactions  t
    JOIN oltp_sales_items   si ON si.transaction_id = t.transaction_id
    WHERE TRUNC(t.transaction_date) = TRUNC(p_load_date)
      -- Avoid duplicates on re-run
      AND NOT EXISTS (
          SELECT 1 FROM fct_sales_olap f
          WHERE f.transaction_id = t.transaction_id
            AND f.product_id     = si.product_id
      );

    v_rows_loaded := SQL%ROWCOUNT;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('ETL complete. Rows loaded: ' || v_rows_loaded);

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ETL failed: ' || SQLERRM);
        RAISE;
END prc_etl_oltp_to_olap;
/


-- =============================================================================
-- SECTION 5: OLAP QUERIES — Window Functions & Analytical Aggregations
-- =============================================================================

-- 5.1 Running total of revenue by month (like a cumulative dashboard)
SELECT
    d.year_num,
    d.month_num,
    d.month_name,
    ROUND(SUM(f.net_amount), 2)                                        AS monthly_revenue,
    ROUND(SUM(SUM(f.net_amount)) OVER (
        PARTITION BY d.year_num ORDER BY d.month_num
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                                                               AS ytd_revenue,
    ROUND(LAG(SUM(f.net_amount), 1) OVER (
        PARTITION BY d.year_num ORDER BY d.month_num
    ), 2)                                                               AS prev_month_revenue,
    ROUND(
        (SUM(f.net_amount) - LAG(SUM(f.net_amount), 1) OVER (
            PARTITION BY d.year_num ORDER BY d.month_num
        )) / NULLIF(LAG(SUM(f.net_amount), 1) OVER (
            PARTITION BY d.year_num ORDER BY d.month_num
        ), 0) * 100, 2
    )                                                                   AS mom_growth_pct
FROM fct_sales_olap f
JOIN dim_date       d ON d.date_id = f.date_id
GROUP BY d.year_num, d.month_num, d.month_name
ORDER BY d.year_num, d.month_num;


-- 5.2 Customer RFM Scoring (Recency, Frequency, Monetary)
-- Classic OLAP pattern for customer analytics
WITH rfm_base AS (
    SELECT
        f.customer_id,
        MAX(d.full_date)                              AS last_purchase_date,
        COUNT(DISTINCT f.transaction_id)              AS frequency,
        ROUND(SUM(f.net_amount), 2)                   AS monetary
    FROM fct_sales_olap f
    JOIN dim_date       d ON d.date_id = f.date_id
    GROUP BY f.customer_id
),
rfm_scored AS (
    SELECT
        customer_id,
        last_purchase_date,
        frequency,
        monetary,
        TRUNC(SYSDATE) - last_purchase_date          AS recency_days,
        NTILE(5) OVER (ORDER BY (TRUNC(SYSDATE) - last_purchase_date))  AS r_score,
        NTILE(5) OVER (ORDER BY frequency)                               AS f_score,
        NTILE(5) OVER (ORDER BY monetary)                                AS m_score
    FROM rfm_base
)
SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    (r_score + f_score + m_score)    AS rfm_total,
    CASE
        WHEN (r_score + f_score + m_score) >= 13 THEN 'Champion'
        WHEN (r_score + f_score + m_score) >= 10 THEN 'Loyal'
        WHEN (r_score + f_score + m_score) >= 7  THEN 'Potential'
        WHEN r_score <= 2                         THEN 'At Risk'
        ELSE 'Need Attention'
    END                              AS customer_label
FROM rfm_scored
ORDER BY rfm_total DESC;


-- 5.3 Product ABC Analysis (Pareto Principle / 80-20 Rule)
WITH product_revenue AS (
    SELECT
        product_id,
        ROUND(SUM(net_amount), 2) AS revenue
    FROM fct_sales_olap
    GROUP BY product_id
),
pareto AS (
    SELECT
        product_id,
        revenue,
        SUM(revenue) OVER ()                                AS total_revenue,
        SUM(revenue) OVER (ORDER BY revenue DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue
    FROM product_revenue
)
SELECT
    product_id,
    revenue,
    ROUND(revenue / total_revenue * 100, 2)                AS pct_of_total,
    ROUND(cumulative_revenue / total_revenue * 100, 2)     AS cumulative_pct,
    CASE
        WHEN cumulative_revenue / total_revenue <= 0.80 THEN 'A'  -- Top 80% revenue
        WHEN cumulative_revenue / total_revenue <= 0.95 THEN 'B'  -- Next 15%
        ELSE                                                   'C'  -- Bottom 5%
    END                                                    AS abc_class
FROM pareto
ORDER BY revenue DESC;


-- 5.4 Store Performance Matrix with PIVOT-like aggregation
SELECT
    store_id,
    ROUND(SUM(net_amount), 2)                             AS total_revenue,
    COUNT(DISTINCT transaction_id)                        AS total_transactions,
    ROUND(AVG(net_amount), 2)                             AS avg_ticket,
    ROUND(SUM(CASE WHEN discount_pct > 0 THEN net_amount END) /
          NULLIF(SUM(net_amount),0) * 100, 2)            AS discounted_revenue_pct,
    RANK() OVER (ORDER BY SUM(net_amount) DESC)           AS revenue_rank,
    DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT transaction_id) DESC) AS volume_rank
FROM fct_sales_olap
GROUP BY store_id
ORDER BY revenue_rank;


-- =============================================================================
-- SECTION 6: ADVANCED PL/SQL PACKAGE — Retail Metrics + ML Integration Bridge
-- =============================================================================

CREATE OR REPLACE PACKAGE pkg_retail_analytics AS
    -- Constants
    c_rfm_champion_threshold  CONSTANT INTEGER := 13;
    c_abc_a_threshold         CONSTANT NUMBER  := 0.80;

    -- Types
    TYPE t_customer_rfm IS RECORD (
        customer_id    INTEGER,
        recency_days   INTEGER,
        frequency      INTEGER,
        monetary       NUMBER,
        label          VARCHAR2(20)
    );

    TYPE t_rfm_table IS TABLE OF t_customer_rfm INDEX BY PLS_INTEGER;

    -- Procedures & Functions
    PROCEDURE refresh_rfm_scores;
    FUNCTION  get_customer_label (p_customer_id IN INTEGER) RETURN VARCHAR2;
    PROCEDURE run_monthly_etl    (p_year IN INTEGER, p_month IN INTEGER);
    FUNCTION  calc_clv           (p_customer_id IN INTEGER, p_months IN INTEGER DEFAULT 12) RETURN NUMBER;
END pkg_retail_analytics;
/

CREATE OR REPLACE PACKAGE BODY pkg_retail_analytics AS

    -- ── Refresh RFM Scores (materialised view pattern) ────────────────────
    PROCEDURE refresh_rfm_scores AS
    BEGIN
        -- Truncate staging table and reload (full refresh pattern)
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rfm_scores_stage';

        INSERT INTO rfm_scores_stage (customer_id, r_score, f_score, m_score, label, refreshed_at)
        WITH rfm_base AS (
            SELECT
                f.customer_id,
                MAX(d.full_date)             AS last_purchase,
                COUNT(DISTINCT f.transaction_id) AS freq,
                SUM(f.net_amount)            AS monetary
            FROM fct_sales_olap f
            JOIN dim_date d ON d.date_id = f.date_id
            GROUP BY f.customer_id
        )
        SELECT
            customer_id,
            NTILE(5) OVER (ORDER BY (TRUNC(SYSDATE) - last_purchase)) AS r_score,
            NTILE(5) OVER (ORDER BY freq)                              AS f_score,
            NTILE(5) OVER (ORDER BY monetary)                          AS m_score,
            CASE
                WHEN (NTILE(5) OVER (ORDER BY (TRUNC(SYSDATE)-last_purchase)) +
                      NTILE(5) OVER (ORDER BY freq) +
                      NTILE(5) OVER (ORDER BY monetary)) >= c_rfm_champion_threshold
                THEN 'Champion'
                ELSE 'Other'
            END,
            SYSDATE
        FROM rfm_base;

        COMMIT;
        DBMS_OUTPUT.PUT_LINE('RFM refresh complete: ' || SQL%ROWCOUNT || ' customers');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE_APPLICATION_ERROR(-20001, 'RFM refresh failed: ' || SQLERRM);
    END refresh_rfm_scores;

    -- ── Get customer RFM label ─────────────────────────────────────────────
    FUNCTION get_customer_label (p_customer_id IN INTEGER) RETURN VARCHAR2 AS
        v_label VARCHAR2(20);
    BEGIN
        SELECT label
        INTO   v_label
        FROM   rfm_scores_stage
        WHERE  customer_id = p_customer_id;

        RETURN v_label;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN 'Unknown';
        WHEN OTHERS THEN        RETURN 'Error';
    END get_customer_label;

    -- ── Monthly ETL loop ───────────────────────────────────────────────────
    PROCEDURE run_monthly_etl (p_year IN INTEGER, p_month IN INTEGER) AS
        v_start_date DATE;
        v_end_date   DATE;
        v_cur_date   DATE;
    BEGIN
        v_start_date := TO_DATE(p_year || LPAD(p_month, 2, '0') || '01', 'YYYYMMDD');
        v_end_date   := LAST_DAY(v_start_date);
        v_cur_date   := v_start_date;

        WHILE v_cur_date <= v_end_date LOOP
            prc_etl_oltp_to_olap(v_cur_date);
            v_cur_date := v_cur_date + 1;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE(
            'Monthly ETL done for ' || p_year || '-' || LPAD(p_month,2,'0')
        );
    END run_monthly_etl;

    -- ── Customer Lifetime Value projection ────────────────────────────────
    FUNCTION calc_clv (p_customer_id IN INTEGER, p_months IN INTEGER DEFAULT 12) RETURN NUMBER AS
        v_avg_monthly_spend NUMBER;
        v_months_active     INTEGER;
        v_clv               NUMBER;
    BEGIN
        SELECT
            NVL(SUM(f.net_amount) / NULLIF(COUNT(DISTINCT d.month_num + d.year_num * 12), 0), 0),
            COUNT(DISTINCT d.month_num + d.year_num * 12)
        INTO v_avg_monthly_spend, v_months_active
        FROM fct_sales_olap f
        JOIN dim_date d ON d.date_id = f.date_id
        WHERE f.customer_id = p_customer_id;

        v_clv := v_avg_monthly_spend * p_months;
        RETURN ROUND(v_clv, 2);
    EXCEPTION
        WHEN OTHERS THEN RETURN 0;
    END calc_clv;

END pkg_retail_analytics;
/


-- =============================================================================
-- SECTION 7: MATERIALIZED VIEWS (OLAP Performance Pattern)
-- =============================================================================
/*
  Materialized Views pre-compute expensive aggregations.
  Instead of GROUP BY + SUM on billions of rows every query,
  the result is stored physically and refreshed on a schedule.
  
  REFRESH options:
    FAST   → only apply changes (requires MLOG$ change log table)
    COMPLETE → full recompute (simpler, slower)
    FORCE  → FAST if possible, else COMPLETE
*/

CREATE MATERIALIZED VIEW mv_monthly_store_revenue
REFRESH COMPLETE ON DEMAND
AS
SELECT
    d.year_num,
    d.month_num,
    f.store_id,
    ROUND(SUM(f.net_amount), 2)          AS revenue,
    COUNT(DISTINCT f.transaction_id)      AS transactions,
    COUNT(DISTINCT f.customer_id)         AS unique_customers
FROM fct_sales_olap f
JOIN dim_date       d ON d.date_id = f.date_id
GROUP BY d.year_num, d.month_num, f.store_id;

-- Refresh manually:
-- EXEC DBMS_MVIEW.REFRESH('mv_monthly_store_revenue', 'C');


-- =============================================================================
-- SECTION 8: PARTITIONING (OLAP Scalability Pattern)
-- =============================================================================
/*
  Range partitioning on date_id allows Oracle (and DuckDB) to skip entire
  partitions during queries that filter on date — called "partition pruning".
  
  Without partitioning: scan ALL rows to find Q4 data.
  With partitioning:    scan ONLY the Q4 partition.
  
  This is one of the most impactful OLAP performance techniques.
*/

CREATE TABLE fct_sales_partitioned (
    sales_key      INTEGER NOT NULL,
    date_id        INTEGER NOT NULL,  -- YYYYMMDD
    customer_id    INTEGER,
    store_id       INTEGER,
    product_id     INTEGER,
    net_amount     NUMBER(12,2)
)
PARTITION BY RANGE (date_id) (
    PARTITION p_2024_q1 VALUES LESS THAN (20240401),
    PARTITION p_2024_q2 VALUES LESS THAN (20240701),
    PARTITION p_2024_q3 VALUES LESS THAN (20241001),
    PARTITION p_2024_q4 VALUES LESS THAN (20250101),
    PARTITION p_2025_q1 VALUES LESS THAN (20250401),
    PARTITION p_future   VALUES LESS THAN (MAXVALUE)
);

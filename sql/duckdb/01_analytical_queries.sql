-- Receita mensal + variação face ao mês anterior
WITH monthly_revenue AS (
    SELECT
        date_trunc('month', transaction_date) AS month_date,
        ROUND(SUM(net_amount), 2) AS revenue
    FROM fct_sales
    GROUP BY 1
)
SELECT
    month_date,
    revenue,
    ROUND(revenue - LAG(revenue) OVER (ORDER BY month_date), 2) AS delta_prev_month,
    ROUND(
        CASE
            WHEN LAG(revenue) OVER (ORDER BY month_date) = 0 THEN NULL
            ELSE (revenue / LAG(revenue) OVER (ORDER BY month_date) - 1) * 100
        END,
        2
    ) AS pct_change
FROM monthly_revenue
ORDER BY month_date;

-- Ranking produtos por categoria
SELECT
    p.category,
    p.product_name,
    ROUND(SUM(f.net_amount), 2) AS revenue,
    RANK() OVER (
        PARTITION BY p.category
        ORDER BY SUM(f.net_amount) DESC
    ) AS category_rank
FROM fct_sales f
JOIN products p ON p.product_id = f.product_id
GROUP BY p.category, p.product_name
QUALIFY category_rank <= 5
ORDER BY p.category, category_rank;

-- Receita mensal por loja
SELECT
    strftime('%Y-%m', t.transaction_date) AS year_month,
    s.store_name,
    ROUND(SUM(si.net_amount), 2) AS revenue
FROM transactions t
JOIN sales_items si ON si.transaction_id = t.transaction_id
JOIN stores s ON s.store_id = t.store_id
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

-- Top 10 produtos por receita
SELECT
    p.product_name,
    p.category,
    ROUND(SUM(si.net_amount), 2) AS revenue,
    SUM(si.quantity) AS units_sold
FROM sales_items si
JOIN products p ON p.product_id = si.product_id
GROUP BY 1, 2
ORDER BY revenue DESC
LIMIT 10;

-- Ticket médio por segmento de cliente
SELECT
    c.segment,
    ROUND(AVG(tx.total_amount), 2) AS avg_ticket
FROM (
    SELECT
        t.transaction_id,
        t.customer_id,
        SUM(si.net_amount) AS total_amount
    FROM transactions t
    JOIN sales_items si ON si.transaction_id = t.transaction_id
    GROUP BY t.transaction_id, t.customer_id
) tx
JOIN customers c ON c.customer_id = tx.customer_id
GROUP BY c.segment
ORDER BY avg_ticket DESC;

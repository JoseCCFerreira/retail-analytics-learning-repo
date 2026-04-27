CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_customer ON transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_store ON transactions(store_id);
CREATE INDEX IF NOT EXISTS idx_sales_items_tx ON sales_items(transaction_id);
CREATE INDEX IF NOT EXISTS idx_sales_items_product ON sales_items(product_id);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);

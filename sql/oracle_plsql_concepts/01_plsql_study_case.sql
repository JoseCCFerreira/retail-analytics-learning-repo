-- Oracle PL/SQL study case (conceitual)

CREATE OR REPLACE PROCEDURE prc_store_revenue (
    p_store_id IN NUMBER,
    p_total_revenue OUT NUMBER
) AS
BEGIN
    SELECT NVL(SUM(net_amount), 0)
    INTO p_total_revenue
    FROM fct_sales
    WHERE store_id = p_store_id;
EXCEPTION
    WHEN OTHERS THEN
        p_total_revenue := 0;
END;
/

CREATE OR REPLACE FUNCTION fn_customer_ltv (
    p_customer_id IN NUMBER
) RETURN NUMBER AS
    v_ltv NUMBER;
BEGIN
    SELECT NVL(SUM(net_amount), 0)
    INTO v_ltv
    FROM fct_sales
    WHERE customer_id = p_customer_id;

    RETURN v_ltv;
END;
/

CREATE OR REPLACE TRIGGER trg_sales_items_audit
AFTER INSERT ON sales_items
FOR EACH ROW
BEGIN
    -- Exemplo simplificado de auditoria
    NULL;
END;
/

CREATE OR REPLACE PACKAGE pkg_retail_metrics AS
    PROCEDURE calculate_store_revenue(p_store_id IN NUMBER, p_total_revenue OUT NUMBER);
    FUNCTION calculate_customer_ltv(p_customer_id IN NUMBER) RETURN NUMBER;
END pkg_retail_metrics;
/

CREATE OR REPLACE PACKAGE BODY pkg_retail_metrics AS
    PROCEDURE calculate_store_revenue(p_store_id IN NUMBER, p_total_revenue OUT NUMBER) AS
    BEGIN
        prc_store_revenue(p_store_id, p_total_revenue);
    END;

    FUNCTION calculate_customer_ltv(p_customer_id IN NUMBER) RETURN NUMBER AS
    BEGIN
        RETURN fn_customer_ltv(p_customer_id);
    END;
END pkg_retail_metrics;
/

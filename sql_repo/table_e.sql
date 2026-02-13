SELECT
    d.order_id,
    CASE
        WHEN d.amount > 1000 THEN 'High Value'
        WHEN d.amount > 500 THEN 'Medium Value'
        ELSE 'Low Value'
    END as order_category,
    d.amount * 1.1 as amount_with_tax
FROM table_d d
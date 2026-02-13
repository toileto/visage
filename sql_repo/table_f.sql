WITH user_stats AS (
    SELECT 
        user_id, 
        SUM(amount) as total_spent,
        COUNT(order_id) as order_count
    FROM table_d
    GROUP BY user_id
)
SELECT 
    u.user_id, 
    u.total_spent,
    CASE 
        WHEN u.total_spent > 5000 AND u.order_count > 10 THEN 'VIP'
        WHEN u.total_spent > 1000 THEN 'Regular'
        ELSE 'New'
    END as customer_segment
FROM user_stats u
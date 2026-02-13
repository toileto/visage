SELECT
    a.id,
    CASE
        WHEN b.status = 'active' THEN a.email
        WHEN b.status = 'inactive' THEN 'no_email'
        ELSE 'unknown'
    END as contact_info
FROM table_a a
JOIN table_b b ON a.id = b.id
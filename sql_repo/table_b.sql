WITH active_users AS (
    SELECT id, name FROM table_a WHERE id > 100
)
SELECT 
    id, 
    name, 
    'active' as status 
FROM active_users
with products AS (
    SELECT * FROM {{ ref("stg_products")}}
),

categories AS (
    SELECT * FROM {{ ref("stg_categories")}}
)

SELECT
    p.*,
    c.category_name
FROM products p
JOIN categories c ON p.category_id = c.category_id
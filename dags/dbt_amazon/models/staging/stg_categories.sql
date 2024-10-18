with categories AS (
    SELECT * FROM {{ source('amazon', 'amazon_categories') }}
)

SELECT
    id as category_id,
    category_name
FROM categories
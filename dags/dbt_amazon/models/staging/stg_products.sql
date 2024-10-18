with products AS (
    SELECT * FROM {{ source('amazon', 'amazon_products') }}
)

SELECT * FROM products
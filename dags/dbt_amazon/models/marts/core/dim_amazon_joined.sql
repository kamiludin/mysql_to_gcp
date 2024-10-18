with dim_amazon_joined AS (
    SELECT * FROM {{ ref('int_amazon_joined') }}
)

SELECT
    asin,
    title,
    imgUrl,
    productURL,
    category_id,
    category_name,
    isBestSeller
FROM
    dim_amazon_joined
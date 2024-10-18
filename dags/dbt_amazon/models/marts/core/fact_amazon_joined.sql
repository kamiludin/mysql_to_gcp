with fact_amazon_joined AS (
    SELECT * FROM {{ ref('int_amazon_joined') }}
)

SELECT
    asin,
    stars,
    reviews,
    price,
    listPrice,
    boughtInLastMonth,
    category_id
FROM
    fact_amazon_joined
WITH dim_amazon_joined AS (
    SELECT * FROM {{ ref('dim_amazon_joined') }}
),
fact_amazon_joined AS (
    SELECT * FROM {{ ref('fact_amazon_joined') }}
)
SELECT
    da.asin,
    da.title,
    da.imgUrl,
    da.productURL,
    da.category_id,
    da.category_name,
    da.isBestSeller,
    fa.price,
    fa.stars
FROM
    dim_amazon_joined da
JOIN
    fact_amazon_joined fa
ON
    da.asin = fa.asin
WHERE
    da.isBestSeller = 'True'
ORDER BY 
    fa.stars DESC
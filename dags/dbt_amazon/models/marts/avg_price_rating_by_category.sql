SELECT
    da.category_name,
    AVG(fa.price) AS avg_price,
    AVG(fa.stars) AS avg_stars
FROM
    {{ ref('dim_amazon_joined') }} da
JOIN
    {{ ref('fact_amazon_joined') }} fa
ON
    da.asin = fa.asin
GROUP BY
    da.category_name
ORDER BY
    avg_stars DESC
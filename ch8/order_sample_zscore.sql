WITH order_count_zscore AS (
    SELECT
        order_date,
        order_count,
        (order_count - avg(order_count) over()
        / (stddev(order_count))) as z_score
    FROM orders_by py
)
SELECT ABS(z_score) AS twosided_score
FROM order_count_zscore
WHERE order_date = (CAST('2020-10-05' AS DATE, - inverval '1 day'))
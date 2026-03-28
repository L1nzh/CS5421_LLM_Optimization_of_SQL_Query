WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '2001-01-12'::DATE
                     AND ('2001-01-12'::DATE + INTERVAL '30 days')::DATE
),
aggregated_web_sales AS (
    SELECT ws_item_sk, SUM(ws_ext_sales_price) AS itemrevenue
    FROM web_sales
    INNER JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
    GROUP BY ws_item_sk
)
SELECT
    i.i_item_id,
    i.i_item_desc,
    i.i_category,
    i.i_class,
    i.i_current_price,
    aws.itemrevenue,
    aws.itemrevenue * 100 / SUM(aws.itemrevenue) OVER (PARTITION BY i.i_class) AS revenueratio
FROM item i
INNER JOIN aggregated_web_sales aws ON i.i_item_sk = aws.ws_item_sk
WHERE i.i_category IN ('Jewelry', 'Sports', 'Books')
ORDER BY
    i.i_category,
    i.i_class,
    i.i_item_id,
    i.i_item_desc,
    revenueratio
LIMIT 100
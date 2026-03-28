WITH ws_item_agg AS (
    SELECT
        ws_item_sk,
        SUM(ws_ext_sales_price) AS itemrevenue
    FROM web_sales
    INNER JOIN date_dim
        ON ws_sold_date_sk = d_date_sk
    WHERE d_date BETWEEN '2001-01-12'::DATE
        AND ('2001-01-12'::DATE + INTERVAL '30 days')::DATE
    GROUP BY ws_item_sk
)
SELECT
    i.i_item_id,
    i.i_item_desc,
    i.i_category,
    i.i_class,
    i.i_current_price,
    w.itemrevenue,
    w.itemrevenue * 100 / SUM(w.itemrevenue) OVER (PARTITION BY i.i_class) AS revenueratio
FROM item i
INNER JOIN ws_item_agg w
    ON i.i_item_sk = w.ws_item_sk
WHERE i.i_category IN ('Jewelry', 'Sports', 'Books')
ORDER BY
    i.i_category,
    i.i_class,
    i.i_item_id,
    i.i_item_desc,
    revenueratio
LIMIT 100
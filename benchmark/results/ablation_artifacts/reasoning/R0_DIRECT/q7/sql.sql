SELECT
  i_item_id,
  avg(ss_quantity) agg1,
  avg(ss_list_price) agg2,
  avg(ss_coupon_amt) agg3,
  avg(ss_sales_price) agg4
FROM store_sales
INNER JOIN date_dim ON ss_sold_date_sk = d_date_sk AND d_year = 2000
INNER JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk AND cd_gender = 'M' AND cd_marital_status = 'S' AND cd_education_status = 'College'
INNER JOIN item ON ss_item_sk = i_item_sk
INNER JOIN promotion ON ss_promo_sk = p_promo_sk AND (p_channel_email = 'N' OR p_channel_event = 'N')
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100
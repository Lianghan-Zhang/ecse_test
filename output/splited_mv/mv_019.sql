-- ============================================================
-- MV: mv_019
-- Fact Table: store_sales
-- Tables: customer_demographics, date_dim, item, store_sales
-- Edges: customer_demographics.cd_demo_sk=store_sales.ss_cdemo_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk
-- QBs: q27.sql::qb::main:0::root, q7.sql::qb::main:0::root
-- Columns: 17 columns
-- ============================================================
CREATE VIEW mv_019 AS
SELECT
  i.i_item_id,
  AVG(ss.ss_quantity) AS agg1,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4
FROM customer_demographics AS cd
INNER JOIN store_sales AS ss
  ON ss.ss_cdemo_sk = cd.cd_demo_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = ss.ss_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
GROUP BY
  i.i_item_id;
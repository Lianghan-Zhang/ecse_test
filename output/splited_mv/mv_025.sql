-- ============================================================
-- MV: mv_025
-- Fact Table: store_sales
-- Tables: item, promotion, store_sales
-- Edges: item.i_item_sk=store_sales.ss_item_sk, promotion.p_promo_sk=store_sales.ss_promo_sk
-- QBs: q61.sql::qb::subquery:1::root.from.subquery1, q64.sql::qb::cte:cross_sales::root.with.cross_sales, q7.sql::qb::main:0::root, q80.sql::qb::cte:ssr::root.with.ssr
-- Columns: 27 columns
-- ============================================================
CREATE VIEW mv_025 AS
SELECT
  i.i_item_id,
  i.i_item_sk,
  i.i_product_name,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  AVG(ss.ss_quantity) AS agg1,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4
FROM item AS i
INNER JOIN store_sales AS ss
  ON ss.ss_item_sk = i.i_item_sk
INNER JOIN promotion AS p
  ON p.p_promo_sk = ss.ss_promo_sk
GROUP BY
  i.i_item_id,
  i.i_item_sk,
  i.i_product_name;
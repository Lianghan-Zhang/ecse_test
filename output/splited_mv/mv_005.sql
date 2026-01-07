-- ============================================================
-- MV: mv_005
-- Fact Table: store_sales
-- Tables: customer, item, promotion, store, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, item.i_item_sk=store_sales.ss_item_sk, promotion.p_promo_sk=store_sales.ss_promo_sk, ... (4 total)
-- QBs: q61.sql::qb::subquery:1::root.from.subquery1, q64.sql::qb::cte:cross_sales::root.with.cross_sales
-- Columns: 32 columns
-- ============================================================
CREATE VIEW mv_005 AS
SELECT
  i.i_item_sk,
  i.i_product_name,
  s.s_store_name,
  s.s_zip,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3,
  SUM(ss.ss_ext_sales_price) AS promotions
FROM customer AS c
INNER JOIN store_sales AS ss
  ON ss.ss_customer_sk = c.c_customer_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN promotion AS p
  ON p.p_promo_sk = ss.ss_promo_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  i.i_item_sk,
  i.i_product_name,
  s.s_store_name,
  s.s_zip;
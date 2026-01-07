-- ============================================================
-- MV: mv_017
-- Fact Table: store_sales
-- Tables: date_dim, item, promotion, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, promotion.p_promo_sk=store_sales.ss_promo_sk
-- QBs: q61.sql::qb::subquery:1::root.from.subquery1, q7.sql::qb::main:0::root, q80.sql::qb::cte:ssr::root.with.ssr
-- Columns: 26 columns
-- ============================================================
CREATE VIEW mv_017 AS
SELECT
  i.i_item_id,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  AVG(ss.ss_quantity) AS agg1,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4
FROM date_dim AS d
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN promotion AS p
  ON p.p_promo_sk = ss.ss_promo_sk
GROUP BY
  i.i_item_id;
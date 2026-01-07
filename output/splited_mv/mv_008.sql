-- ============================================================
-- MV: mv_008
-- Fact Table: store_sales
-- Tables: date_dim, item, promotion, store, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, promotion.p_promo_sk=store_sales.ss_promo_sk, ... (4 total)
-- QBs: q61.sql::qb::subquery:1::root.from.subquery1, q80.sql::qb::cte:ssr::root.with.ssr
-- Columns: 22 columns
-- ============================================================
CREATE VIEW mv_008 AS
SELECT
  s.s_store_id,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price
FROM date_dim AS d
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN promotion AS p
  ON p.p_promo_sk = ss.ss_promo_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  s.s_store_id;
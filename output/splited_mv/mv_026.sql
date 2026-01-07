-- ============================================================
-- MV: mv_026
-- Fact Table: store_sales
-- Tables: date_dim, item, store_sales
-- Edges: d1.d_date_sk=store_sales.ss_sold_date_sk, iss.i_item_sk=store_sales.ss_item_sk
-- QBs: q14a.sql::qb::union_branch:1::root.with.cross_items.join.subquery1.intersect.left.intersect.left, q14b.sql::qb::union_branch:1::root.with.cross_items.join.subquery1.intersect.left.intersect.left, q36.sql::qb::main:0::root
-- Columns: 14 columns
-- ============================================================
CREATE VIEW mv_026 AS
SELECT
  SUM(ss.ss_net_profit) AS sum_ss__ss_net_profit,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price
FROM date_dim AS d1
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d1.d_date_sk
INNER JOIN item AS iss
  ON iss.i_item_sk = ss.ss_item_sk;
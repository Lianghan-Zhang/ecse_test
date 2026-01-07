-- ============================================================
-- MV: mv_030
-- Fact Table: store_sales
-- Tables: date_dim, store_sales
-- Edges: d1.d_date_sk=store_sales.ss_sold_date_sk
-- QBs: q14a.sql::qb::union_branch:1::root.with.cross_items.join.subquery1.intersect.left.intersect.left, q14b.sql::qb::union_branch:1::root.with.cross_items.join.subquery1.intersect.left.intersect.left, q36.sql::qb::main:0::root, q50.sql::qb::main:0::root, q64.sql::qb::cte:cross_sales::root.with.cross_sales, ... (6 total)
-- Columns: 22 columns
-- ============================================================
CREATE VIEW mv_030 AS
SELECT
  d1.d_year AS d1__d_year,
  d2.d_year AS d2__d_year,
  d3.d_year AS d3__d_year,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3,
  SUM(ss.ss_net_profit) AS sum_ss__ss_net_profit,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price
FROM date_dim AS d1
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d1.d_date_sk
GROUP BY
  d1.d_year,
  d2.d_year,
  d3.d_year;
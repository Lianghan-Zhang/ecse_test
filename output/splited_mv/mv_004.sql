-- ============================================================
-- MV: mv_004
-- Fact Table: store_sales
-- Tables: date_dim, item, store_returns, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, store_sales.ss_item_sk=store_returns.sr_item_sk, ... (4 total)
-- QBs: q75.sql::qb::union_branch:2::root.with.all_sales.from.subquery1.union.left.union.right, q78.sql::qb::cte:ss::root.with.ss, q80.sql::qb::cte:ssr::root.with.ssr
-- Columns: 26 columns
-- ============================================================
CREATE VIEW mv_004 AS
SELECT
  d.d_year,
  ss.ss_customer_sk,
  ss.ss_item_sk,
  SUM(ss.ss_quantity) AS ss_qty,
  SUM(ss.ss_wholesale_cost) AS ss_wc,
  SUM(ss.ss_sales_price) AS ss_sp,
  SUM(ss.ss_ext_sales_price) AS sales
FROM date_dim AS d
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d.d_date_sk
LEFT JOIN store_returns AS sr
  ON sr.sr_item_sk = ss.ss_item_sk
GROUP BY
  d.d_year,
  ss.ss_customer_sk,
  ss.ss_item_sk;
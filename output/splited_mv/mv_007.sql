-- ============================================================
-- MV: mv_007
-- Fact Table: store_sales
-- Tables: date_dim, store, store_returns, store_sales
-- Edges: d1.d_date_sk=store_sales.ss_sold_date_sk, store.s_store_sk=store_sales.ss_store_sk, store_returns.sr_item_sk=store_sales.ss_item_sk, ... (4 total)
-- QBs: q50.sql::qb::main:0::root, q64.sql::qb::cte:cross_sales::root.with.cross_sales
-- Columns: 34 columns
-- ============================================================
CREATE VIEW mv_007 AS
SELECT
  d1.d_year AS d1__d_year,
  d2.d_year AS d2__d_year,
  d3.d_year AS d3__d_year,
  s.s_city,
  s.s_company_id,
  s.s_county,
  s.s_state,
  s.s_store_name,
  s.s_street_name,
  s.s_street_number,
  s.s_street_type,
  s.s_suite_number,
  s.s_zip,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3
FROM date_dim AS d1
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d1.d_date_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
INNER JOIN store_returns AS sr
  ON sr.sr_item_sk = ss.ss_item_sk
GROUP BY
  d1.d_year,
  d2.d_year,
  d3.d_year,
  s.s_city,
  s.s_company_id,
  s.s_county,
  s.s_state,
  s.s_store_name,
  s.s_street_name,
  s.s_street_number,
  s.s_street_type,
  s.s_suite_number,
  s.s_zip;
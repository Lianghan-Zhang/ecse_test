-- ============================================================
-- MV: mv_016
-- Fact Table: store_sales
-- Tables: store, store_returns, store_sales
-- Edges: store.s_store_sk=store_sales.ss_store_sk, store_returns.sr_item_sk=store_sales.ss_item_sk, store_returns.sr_ticket_number=store_sales.ss_ticket_number
-- QBs: q24a.sql::qb::cte:ssales::root.with.ssales, q24b.sql::qb::cte:ssales::root.with.ssales, q50.sql::qb::main:0::root, q64.sql::qb::cte:cross_sales::root.with.cross_sales
-- Columns: 29 columns
-- ============================================================
CREATE VIEW mv_016 AS
SELECT
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
  SUM(ss.ss_coupon_amt) AS s3,
  SUM(ss.ss_net_paid) AS netpaid
FROM store AS s
INNER JOIN store_sales AS ss
  ON ss.ss_store_sk = s.s_store_sk
INNER JOIN store_returns AS sr
  ON sr.sr_item_sk = ss.ss_item_sk
GROUP BY
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
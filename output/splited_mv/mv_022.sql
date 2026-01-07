-- ============================================================
-- MV: mv_022
-- Fact Table: store_sales
-- Tables: household_demographics, store, store_sales
-- Edges: household_demographics.hd_demo_sk=store_sales.ss_hdemo_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q34.sql::qb::subquery:1::root.from.subquery1, q46.sql::qb::subquery:1::root.from.subquery1, q68.sql::qb::subquery:1::root.from.subquery1, q73.sql::qb::subquery:1::root.from.subquery1, q79.sql::qb::subquery:1::root.from.subquery1, ... (14 total)
-- Columns: 21 columns
-- ============================================================
CREATE VIEW mv_022 AS
SELECT
  s.s_city,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number,
  COUNT(*) AS count_all,
  SUM(ss.ss_coupon_amt) AS amt,
  SUM(ss.ss_net_profit) AS profit,
  SUM(ss.ss_ext_sales_price) AS extended_price,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax
FROM household_demographics AS hd
INNER JOIN store_sales AS ss
  ON ss.ss_hdemo_sk = hd.hd_demo_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  s.s_city,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number;
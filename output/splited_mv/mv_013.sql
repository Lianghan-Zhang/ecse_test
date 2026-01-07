-- ============================================================
-- MV: mv_013
-- Fact Table: store_sales
-- Tables: date_dim, household_demographics, store, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, household_demographics.hd_demo_sk=store_sales.ss_hdemo_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q34.sql::qb::subquery:1::root.from.subquery1, q46.sql::qb::subquery:1::root.from.subquery1, q68.sql::qb::subquery:1::root.from.subquery1, q73.sql::qb::subquery:1::root.from.subquery1, q79.sql::qb::subquery:1::root.from.subquery1
-- Columns: 23 columns
-- ============================================================
CREATE VIEW mv_013 AS
SELECT
  s.s_city,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number,
  COUNT(*) AS cnt,
  SUM(ss.ss_ext_sales_price) AS extended_price,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax,
  SUM(ss.ss_coupon_amt) AS amt,
  SUM(ss.ss_net_profit) AS profit
FROM date_dim AS d
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d.d_date_sk
INNER JOIN household_demographics AS hd
  ON hd.hd_demo_sk = ss.ss_hdemo_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  s.s_city,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number;
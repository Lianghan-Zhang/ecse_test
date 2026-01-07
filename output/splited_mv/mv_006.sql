-- ============================================================
-- MV: mv_006
-- Fact Table: store_sales
-- Tables: customer_address, date_dim, household_demographics, store, store_sales
-- Edges: customer_address.ca_address_sk=store_sales.ss_addr_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, household_demographics.hd_demo_sk=store_sales.ss_hdemo_sk, ... (4 total)
-- QBs: q46.sql::qb::subquery:1::root.from.subquery1, q68.sql::qb::subquery:1::root.from.subquery1
-- Columns: 22 columns
-- ============================================================
CREATE VIEW mv_006 AS
SELECT
  ca.ca_city,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number,
  SUM(ss.ss_coupon_amt) AS amt,
  SUM(ss.ss_net_profit) AS profit,
  SUM(ss.ss_ext_sales_price) AS extended_price,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax
FROM customer_address AS ca
INNER JOIN store_sales AS ss
  ON ss.ss_addr_sk = ca.ca_address_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = ss.ss_sold_date_sk
INNER JOIN household_demographics AS hd
  ON hd.hd_demo_sk = ss.ss_hdemo_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  ca.ca_city,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number;
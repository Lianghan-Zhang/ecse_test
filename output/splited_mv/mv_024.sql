-- ============================================================
-- MV: mv_024
-- Fact Table: store_sales
-- Tables: customer_address, date_dim, store_sales
-- Edges: customer_address.ca_address_sk=store_sales.ss_addr_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk
-- QBs: q31.sql::qb::cte:ss::root.with.ss, q33.sql::qb::cte:ss::root.with.ss, q46.sql::qb::subquery:1::root.from.subquery1, q56.sql::qb::cte:ss::root.with.ss, q60.sql::qb::cte:ss::root.with.ss, ... (6 total)
-- Columns: 22 columns
-- ============================================================
CREATE VIEW mv_024 AS
SELECT
  ca.ca_city,
  ca.ca_county,
  d.d_qoy,
  d.d_year,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax,
  SUM(ss.ss_coupon_amt) AS amt,
  SUM(ss.ss_net_profit) AS profit
FROM customer_address AS ca
INNER JOIN store_sales AS ss
  ON ss.ss_addr_sk = ca.ca_address_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = ss.ss_sold_date_sk
GROUP BY
  ca.ca_city,
  ca.ca_county,
  d.d_qoy,
  d.d_year,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number;
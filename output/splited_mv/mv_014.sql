-- ============================================================
-- MV: mv_014
-- Fact Table: store_sales
-- Tables: customer_address, date_dim, item, store_sales
-- Edges: customer_address.ca_address_sk=store_sales.ss_addr_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk
-- QBs: q31.sql::qb::cte:ss::root.with.ss, q33.sql::qb::cte:ss::root.with.ss, q56.sql::qb::cte:ss::root.with.ss, q60.sql::qb::cte:ss::root.with.ss
-- Columns: 14 columns
-- ============================================================
CREATE VIEW mv_014 AS
SELECT
  ca.ca_county,
  d.d_qoy,
  d.d_year,
  i.i_item_id,
  i.i_manufact_id,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price
FROM customer_address AS ca
INNER JOIN store_sales AS ss
  ON ss.ss_addr_sk = ca.ca_address_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = ss.ss_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
GROUP BY
  ca.ca_county,
  d.d_qoy,
  d.d_year,
  i.i_item_id,
  i.i_manufact_id;
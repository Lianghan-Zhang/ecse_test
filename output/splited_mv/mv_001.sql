-- ============================================================
-- MV: mv_001
-- Fact Table: store_sales
-- Tables: customer, customer_address, item, store, store_returns, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, customer_address.ca_zip=store.s_zip, item.i_item_sk=store_sales.ss_item_sk, ... (6 total)
-- QBs: q24a.sql::qb::cte:ssales::root.with.ssales, q24b.sql::qb::cte:ssales::root.with.ssales
-- Columns: 25 columns
-- ============================================================
CREATE VIEW mv_001 AS
SELECT
  c.c_first_name,
  c.c_last_name,
  ca.ca_state,
  i.i_color,
  i.i_current_price,
  i.i_manager_id,
  i.i_size,
  i.i_units,
  s.s_state,
  s.s_store_name,
  SUM(ss.ss_net_paid) AS netpaid
FROM customer AS c
INNER JOIN store_sales AS ss
  ON ss.ss_customer_sk = c.c_customer_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
INNER JOIN customer_address AS ca
  ON ca.ca_zip = s.s_zip
INNER JOIN store_returns AS sr
  ON sr.sr_ticket_number = ss.ss_ticket_number
GROUP BY
  c.c_first_name,
  c.c_last_name,
  ca.ca_state,
  i.i_color,
  i.i_current_price,
  i.i_manager_id,
  i.i_size,
  i.i_units,
  s.s_state,
  s.s_store_name;
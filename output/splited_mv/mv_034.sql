-- ============================================================
-- MV: mv_034
-- Fact Table: customer
-- Tables: customer, customer_address
-- Edges: current_addr.ca_address_sk=customer.c_current_addr_sk
-- QBs: q46.sql::qb::main:0::root, q68.sql::qb::main:0::root
-- Columns: 7 columns
-- ============================================================
CREATE VIEW mv_034 AS
SELECT
  current_addr.ca_address_sk,
  current_addr.ca_city AS current_addr__ca_city,
  c.c_current_addr_sk,
  c.c_customer_sk,
  c.c_first_name,
  c.c_last_name,
  customer_address.ca_city AS customer_address__ca_city
FROM customer_address AS current_addr
INNER JOIN customer AS c
  ON c.c_current_addr_sk = current_addr.ca_address_sk;
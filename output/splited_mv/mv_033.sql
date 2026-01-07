-- ============================================================
-- MV: mv_033
-- Fact Table: customer
-- Tables: customer, customer_address
-- Edges: customer.c_current_addr_sk=customer_address.ca_address_sk
-- QBs: q30.sql::qb::main:0::root, q8.sql::qb::subquery:3::root.join.subquery1.from.subquery2.intersect.right.from.subquery3, q81.sql::qb::main:0::root
-- Columns: 26 columns
-- ============================================================
CREATE VIEW mv_033 AS
SELECT
  ca.ca_zip,
  COUNT(*) AS cnt
FROM customer AS c
INNER JOIN customer_address AS ca
  ON ca.ca_address_sk = c.c_current_addr_sk
GROUP BY
  ca.ca_zip;
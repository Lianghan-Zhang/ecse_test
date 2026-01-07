-- ============================================================
-- MV: mv_049
-- Fact Table: catalog_sales
-- Tables: catalog_sales, customer, customer_address, date_dim
-- Edges: catalog_sales.cs_bill_customer_sk=customer.c_customer_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk, customer.c_current_addr_sk=customer_address.ca_address_sk
-- QBs: q15.sql::qb::main:0::root, q18.sql::qb::main:0::root
-- Columns: 22 columns
-- ============================================================
CREATE VIEW mv_049 AS
SELECT
  ca.ca_zip,
  SUM(cs.cs_sales_price) AS sum_cs__cs_sales_price
FROM catalog_sales AS cs
INNER JOIN customer AS c
  ON c.c_customer_sk = cs.cs_bill_customer_sk
INNER JOIN customer_address AS ca
  ON ca.ca_address_sk = c.c_current_addr_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = cs.cs_sold_date_sk
GROUP BY
  ca.ca_zip;
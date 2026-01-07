-- ============================================================
-- MV: mv_052
-- Fact Table: catalog_sales
-- Tables: catalog_sales, customer, date_dim
-- Edges: catalog_sales.cs_bill_customer_sk=customer.c_customer_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk
-- QBs: q15.sql::qb::main:0::root, q18.sql::qb::main:0::root, q23b.sql::qb::union_branch:1::root.from.subquery3.union.left, q38.sql::qb::union_branch:2::root.from.subquery1.intersect.left.intersect.right, q4.sql::qb::union_branch:2::root.with.year_total.union.left.union.right, ... (6 total)
-- Columns: 31 columns
-- ============================================================
CREATE VIEW mv_052 AS
SELECT
  c.c_birth_country,
  c.c_customer_id,
  c.c_email_address,
  c.c_first_name,
  c.c_last_name,
  c.c_login,
  c.c_preferred_cust_flag,
  d.d_year,
  SUM(cs.cs_sales_price) AS sum_cs__cs_sales_price
FROM catalog_sales AS cs
INNER JOIN customer AS c
  ON c.c_customer_sk = cs.cs_bill_customer_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = cs.cs_sold_date_sk
GROUP BY
  c.c_birth_country,
  c.c_customer_id,
  c.c_email_address,
  c.c_first_name,
  c.c_last_name,
  c.c_login,
  c.c_preferred_cust_flag,
  d.d_year;
-- ============================================================
-- MV: mv_023
-- Fact Table: store_sales
-- Tables: customer, date_dim, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk
-- QBs: q11.sql::qb::union_branch:1::root.with.year_total.union.left, q19.sql::qb::main:0::root, q23a.sql::qb::subquery:1::root.with.max_store_sales.from.subquery1, q23b.sql::qb::subquery:1::root.with.max_store_sales.from.subquery1, q38.sql::qb::union_branch:1::root.from.subquery1.intersect.left.intersect.left, ... (10 total)
-- Columns: 26 columns
-- ============================================================
CREATE VIEW mv_023 AS
SELECT
  c.c_birth_country,
  c.c_customer_id,
  c.c_customer_sk,
  c.c_email_address,
  c.c_first_name,
  c.c_last_name,
  c.c_login,
  c.c_preferred_cust_flag,
  d.d_year,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  SUM(ss.ss_net_paid) AS year_total
FROM customer AS c
INNER JOIN store_sales AS ss
  ON ss.ss_customer_sk = c.c_customer_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = ss.ss_sold_date_sk
GROUP BY
  c.c_birth_country,
  c.c_customer_id,
  c.c_customer_sk,
  c.c_email_address,
  c.c_first_name,
  c.c_last_name,
  c.c_login,
  c.c_preferred_cust_flag,
  d.d_year;
-- ============================================================
-- MV: mv_029
-- Fact Table: store_sales
-- Tables: customer, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk
-- QBs: q11.sql::qb::union_branch:1::root.with.year_total.union.left, q19.sql::qb::main:0::root, q23a.sql::qb::cte:best_ss_customer::root.with.best_ss_customer, q23a.sql::qb::subquery:1::root.with.max_store_sales.from.subquery1, q23b.sql::qb::cte:best_ss_customer::root.with.best_ss_customer, ... (15 total)
-- Columns: 32 columns
-- ============================================================
CREATE VIEW mv_029 AS
SELECT
  c.c_birth_country,
  c.c_customer_id,
  c.c_customer_sk,
  c.c_email_address,
  c.c_first_name,
  c.c_last_name,
  c.c_login,
  c.c_preferred_cust_flag,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  SUM(ss.ss_net_paid) AS sum_ss__ss_net_paid
FROM customer AS c
INNER JOIN store_sales AS ss
  ON ss.ss_customer_sk = c.c_customer_sk
GROUP BY
  c.c_birth_country,
  c.c_customer_id,
  c.c_customer_sk,
  c.c_email_address,
  c.c_first_name,
  c.c_last_name,
  c.c_login,
  c.c_preferred_cust_flag;
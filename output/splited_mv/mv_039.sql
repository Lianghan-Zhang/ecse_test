-- ============================================================
-- MV: mv_039
-- Fact Table: web_sales
-- Tables: customer, date_dim, web_sales
-- Edges: customer.c_customer_sk=web_sales.ws_bill_customer_sk, date_dim.d_date_sk=web_sales.ws_sold_date_sk
-- QBs: q11.sql::qb::union_branch:2::root.with.year_total.union.right, q23b.sql::qb::union_branch:2::root.from.subquery3.union.right, q38.sql::qb::union_branch:3::root.from.subquery1.intersect.right, q4.sql::qb::union_branch:3::root.with.year_total.union.right, q45.sql::qb::main:0::root, ... (7 total)
-- Columns: 26 columns
-- ============================================================
CREATE VIEW mv_039 AS
SELECT
  c.c_birth_country,
  c.c_customer_id,
  c.c_email_address,
  c.c_first_name,
  c.c_last_name,
  c.c_login,
  c.c_preferred_cust_flag,
  d.d_year,
  SUM(ws.ws_sales_price) AS sum_ws__ws_sales_price,
  SUM(ws.ws_net_paid) AS year_total
FROM customer AS c
INNER JOIN web_sales AS ws
  ON ws.ws_bill_customer_sk = c.c_customer_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = ws.ws_sold_date_sk
GROUP BY
  c.c_birth_country,
  c.c_customer_id,
  c.c_email_address,
  c.c_first_name,
  c.c_last_name,
  c.c_login,
  c.c_preferred_cust_flag,
  d.d_year;
-- ECSE Candidate Materialized Views
-- Generated: 2026-01-06T16:08:46.471881
-- Dialect: spark
-- Total MVs: 66

-- ============================================================
-- MV: mv_001
-- Fact Table: store_sales
-- Tables: catalog_sales, date_dim, item, store, store_returns, store_sales
-- Edges: catalog_sales.cs_bill_customer_sk=store_returns.sr_customer_sk, catalog_sales.cs_item_sk=store_returns.sr_item_sk, catalog_sales.cs_sold_date_sk=d3.d_date_sk, ... (10 total)
-- QBs: q17.sql::qb::main:0::root, q25.sql::qb::main:0::root, q29.sql::qb::main:0::root
-- Columns: 37 columns
-- ============================================================
CREATE VIEW mv_001 AS
SELECT
  i.i_item_desc,
  i.i_item_id,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  COUNT(ss.ss_quantity) AS store_sales_quantitycount,
  AVG(ss.ss_quantity) AS avg_ss__ss_quantity,
  STDDEV_SAMP(ss.ss_quantity) AS stddev_samp_ss__ss_quantity,
  COUNT(sr.sr_return_quantity) AS as_store_returns_quantitycount,
  AVG(sr.sr_return_quantity) AS avg_sr__sr_return_quantity,
  STDDEV_SAMP(sr.sr_return_quantity) AS stddev_samp_sr__sr_return_quantity,
  COUNT(cs.cs_quantity) AS catalog_sales_quantitycount,
  AVG(cs.cs_quantity) AS avg_cs__cs_quantity,
  STDDEV_SAMP(cs.cs_quantity) AS stddev_samp_cs__cs_quantity,
  SUM(ss.ss_net_profit) AS store_sales_profit,
  SUM(sr.sr_net_loss) AS store_returns_loss,
  SUM(cs.cs_net_profit) AS catalog_sales_profit,
  SUM(ss.ss_quantity) AS store_sales_quantity,
  SUM(sr.sr_return_quantity) AS store_returns_quantity,
  SUM(cs.cs_quantity) AS catalog_sales_quantity
FROM catalog_sales AS cs
INNER JOIN date_dim AS d3
  ON d3.d_date_sk = cs.cs_sold_date_sk
INNER JOIN store_returns AS sr
  ON sr.sr_customer_sk = cs.cs_bill_customer_sk AND sr.sr_item_sk = cs.cs_item_sk
INNER JOIN date_dim AS d2
  ON d2.d_date_sk = sr.sr_returned_date_sk
INNER JOIN store_sales AS ss
  ON ss.ss_customer_sk = sr.sr_customer_sk
  AND ss.ss_item_sk = sr.sr_item_sk
  AND ss.ss_ticket_number = sr.sr_ticket_number
INNER JOIN date_dim AS d1
  ON d1.d_date_sk = ss.ss_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  i.i_item_desc,
  i.i_item_id,
  s.s_state,
  s.s_store_id,
  s.s_store_name;

-- ============================================================
-- MV: mv_002
-- Fact Table: store_sales
-- Tables: date_dim, store, store_returns, store_sales
-- Edges: d1.d_date_sk=store_sales.ss_sold_date_sk, d2.d_date_sk=store_returns.sr_returned_date_sk, store.s_store_sk=store_sales.ss_store_sk, ... (6 total)
-- QBs: q17.sql::qb::main:0::root, q25.sql::qb::main:0::root, q29.sql::qb::main:0::root, q50.sql::qb::main:0::root
-- Columns: 33 columns
-- ============================================================
CREATE VIEW mv_002 AS
SELECT
  s.s_city,
  s.s_company_id,
  s.s_county,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  s.s_street_name,
  s.s_street_number,
  s.s_street_type,
  s.s_suite_number,
  s.s_zip,
  COUNT(ss.ss_quantity) AS store_sales_quantitycount,
  AVG(ss.ss_quantity) AS avg_ss__ss_quantity,
  STDDEV_SAMP(ss.ss_quantity) AS stddev_samp_ss__ss_quantity,
  COUNT(sr.sr_return_quantity) AS as_store_returns_quantitycount,
  AVG(sr.sr_return_quantity) AS avg_sr__sr_return_quantity,
  STDDEV_SAMP(sr.sr_return_quantity) AS stddev_samp_sr__sr_return_quantity,
  SUM(ss.ss_net_profit) AS store_sales_profit,
  SUM(sr.sr_net_loss) AS store_returns_loss,
  SUM(ss.ss_quantity) AS store_sales_quantity,
  SUM(sr.sr_return_quantity) AS store_returns_quantity
FROM date_dim AS d1
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d1.d_date_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
INNER JOIN store_returns AS sr
  ON sr.sr_customer_sk = ss.ss_customer_sk
  AND sr.sr_item_sk = ss.ss_item_sk
  AND sr.sr_ticket_number = ss.ss_ticket_number
INNER JOIN date_dim AS d2
  ON d2.d_date_sk = sr.sr_returned_date_sk
GROUP BY
  s.s_city,
  s.s_company_id,
  s.s_county,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  s.s_street_name,
  s.s_street_number,
  s.s_street_type,
  s.s_suite_number,
  s.s_zip;

-- ============================================================
-- MV: mv_003
-- Fact Table: store_sales
-- Tables: customer, customer_address, item, store, store_returns, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, customer_address.ca_zip=store.s_zip, item.i_item_sk=store_sales.ss_item_sk, ... (6 total)
-- QBs: q24a.sql::qb::cte:ssales::root.with.ssales, q24b.sql::qb::cte:ssales::root.with.ssales
-- Columns: 25 columns
-- ============================================================
CREATE VIEW mv_003 AS
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
  ON sr.sr_item_sk = ss.ss_item_sk AND sr.sr_ticket_number = ss.ss_ticket_number
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

-- ============================================================
-- MV: mv_004
-- Fact Table: store_sales
-- Tables: date_dim, item, store, store_returns, store_sales
-- Edges: d1.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, store.s_store_sk=store_sales.ss_store_sk, ... (5 total)
-- QBs: q17.sql::qb::main:0::root, q25.sql::qb::main:0::root, q29.sql::qb::main:0::root, q50.sql::qb::main:0::root, q64.sql::qb::cte:cross_sales::root.with.cross_sales
-- Columns: 42 columns
-- ============================================================
CREATE VIEW mv_004 AS
SELECT
  d1.d_year,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  i.i_product_name,
  s.s_city,
  s.s_company_id,
  s.s_county,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  s.s_street_name,
  s.s_street_number,
  s.s_street_type,
  s.s_suite_number,
  s.s_zip,
  COUNT(ss.ss_quantity) AS store_sales_quantitycount,
  AVG(ss.ss_quantity) AS avg_ss__ss_quantity,
  STDDEV_SAMP(ss.ss_quantity) AS stddev_samp_ss__ss_quantity,
  COUNT(sr.sr_return_quantity) AS as_store_returns_quantitycount,
  AVG(sr.sr_return_quantity) AS avg_sr__sr_return_quantity,
  STDDEV_SAMP(sr.sr_return_quantity) AS stddev_samp_sr__sr_return_quantity,
  SUM(ss.ss_net_profit) AS store_sales_profit,
  SUM(sr.sr_net_loss) AS store_returns_loss,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3,
  SUM(ss.ss_quantity) AS store_sales_quantity,
  SUM(sr.sr_return_quantity) AS store_returns_quantity
FROM date_dim AS d1
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d1.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
INNER JOIN store_returns AS sr
  ON sr.sr_item_sk = ss.ss_item_sk AND sr.sr_ticket_number = ss.ss_ticket_number
GROUP BY
  d1.d_year,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  i.i_product_name,
  s.s_city,
  s.s_company_id,
  s.s_county,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  s.s_street_name,
  s.s_street_number,
  s.s_street_type,
  s.s_suite_number,
  s.s_zip;

-- ============================================================
-- MV: mv_005
-- Fact Table: store_sales
-- Tables: customer, customer_address, date_dim, item, store, store_sales
-- Edges: customer.c_current_addr_sk=customer_address.ca_address_sk, customer.c_customer_sk=store_sales.ss_customer_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, ... (5 total)
-- QBs: q19.sql::qb::main:0::root, q61.sql::qb::subquery:1::root.from.subquery1, q61.sql::qb::subquery:2::root.join.subquery2
-- Columns: 24 columns
-- ============================================================
CREATE VIEW mv_005 AS
SELECT
  i.i_brand,
  i.i_brand_id,
  i.i_manufact,
  i.i_manufact_id,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price
FROM customer AS c
INNER JOIN customer_address AS ca
  ON ca.ca_address_sk = c.c_current_addr_sk
INNER JOIN store_sales AS ss
  ON ss.ss_customer_sk = c.c_customer_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = ss.ss_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  i.i_brand,
  i.i_brand_id,
  i.i_manufact,
  i.i_manufact_id;

-- ============================================================
-- MV: mv_006
-- Fact Table: store_sales
-- Tables: customer, item, store, store_returns, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, item.i_item_sk=store_sales.ss_item_sk, store.s_store_sk=store_sales.ss_store_sk, ... (5 total)
-- QBs: q24a.sql::qb::cte:ssales::root.with.ssales, q24b.sql::qb::cte:ssales::root.with.ssales, q64.sql::qb::cte:cross_sales::root.with.cross_sales
-- Columns: 36 columns
-- ============================================================
CREATE VIEW mv_006 AS
SELECT
  c.c_first_name,
  c.c_last_name,
  i.i_color,
  i.i_current_price,
  i.i_item_sk,
  i.i_manager_id,
  i.i_product_name,
  i.i_size,
  i.i_units,
  s.s_state,
  s.s_store_name,
  s.s_zip,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3,
  SUM(ss.ss_net_paid) AS netpaid
FROM customer AS c
INNER JOIN store_sales AS ss
  ON ss.ss_customer_sk = c.c_customer_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
INNER JOIN store_returns AS sr
  ON sr.sr_item_sk = ss.ss_item_sk AND sr.sr_ticket_number = ss.ss_ticket_number
GROUP BY
  c.c_first_name,
  c.c_last_name,
  i.i_color,
  i.i_current_price,
  i.i_item_sk,
  i.i_manager_id,
  i.i_product_name,
  i.i_size,
  i.i_units,
  s.s_state,
  s.s_store_name,
  s.s_zip;

-- ============================================================
-- MV: mv_007
-- Fact Table: store_sales
-- Tables: item, store, store_returns, store_sales
-- Edges: item.i_item_sk=store_sales.ss_item_sk, store.s_store_sk=store_sales.ss_store_sk, store_returns.sr_item_sk=store_sales.ss_item_sk, ... (4 total)
-- QBs: q17.sql::qb::main:0::root, q24a.sql::qb::cte:ssales::root.with.ssales, q24b.sql::qb::cte:ssales::root.with.ssales, q25.sql::qb::main:0::root, q29.sql::qb::main:0::root, ... (7 total)
-- Columns: 43 columns
-- ============================================================
CREATE VIEW mv_007 AS
SELECT
  i.i_color,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  i.i_manager_id,
  i.i_product_name,
  i.i_size,
  i.i_units,
  s.s_city,
  s.s_company_id,
  s.s_county,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  s.s_street_name,
  s.s_street_number,
  s.s_street_type,
  s.s_suite_number,
  s.s_zip,
  COUNT(ss.ss_quantity) AS store_sales_quantitycount,
  AVG(ss.ss_quantity) AS avg_ss__ss_quantity,
  STDDEV_SAMP(ss.ss_quantity) AS stddev_samp_ss__ss_quantity,
  COUNT(sr.sr_return_quantity) AS as_store_returns_quantitycount,
  AVG(sr.sr_return_quantity) AS avg_sr__sr_return_quantity,
  STDDEV_SAMP(sr.sr_return_quantity) AS stddev_samp_sr__sr_return_quantity,
  SUM(ss.ss_net_paid) AS netpaid,
  SUM(ss.ss_net_profit) AS store_sales_profit,
  SUM(sr.sr_net_loss) AS store_returns_loss,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3,
  SUM(ss.ss_quantity) AS store_sales_quantity,
  SUM(sr.sr_return_quantity) AS store_returns_quantity
FROM item AS i
INNER JOIN store_sales AS ss
  ON ss.ss_item_sk = i.i_item_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
INNER JOIN store_returns AS sr
  ON sr.sr_item_sk = ss.ss_item_sk AND sr.sr_ticket_number = ss.ss_ticket_number
GROUP BY
  i.i_color,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  i.i_manager_id,
  i.i_product_name,
  i.i_size,
  i.i_units,
  s.s_city,
  s.s_company_id,
  s.s_county,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  s.s_street_name,
  s.s_street_number,
  s.s_street_type,
  s.s_suite_number,
  s.s_zip;

-- ============================================================
-- MV: mv_008
-- Fact Table: store_sales
-- Tables: date_dim, item, store_returns, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, store_sales.ss_item_sk=store_returns.sr_item_sk, ... (4 total)
-- QBs: q75.sql::qb::union_branch:2::root.with.all_sales.from.subquery1.union.left.union.right, q78.sql::qb::cte:ss::root.with.ss, q80.sql::qb::cte:ssr::root.with.ssr
-- Columns: 26 columns
-- ============================================================
CREATE VIEW mv_008 AS
SELECT
  d.d_year,
  ss.ss_customer_sk,
  ss.ss_item_sk,
  SUM(ss.ss_quantity) AS ss_qty,
  SUM(ss.ss_wholesale_cost) AS ss_wc,
  SUM(ss.ss_sales_price) AS ss_sp,
  SUM(ss.ss_ext_sales_price) AS sales
FROM date_dim AS d
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
LEFT JOIN store_returns AS sr
  ON sr.sr_item_sk = ss.ss_item_sk AND sr.sr_ticket_number = ss.ss_ticket_number
GROUP BY
  d.d_year,
  ss.ss_customer_sk,
  ss.ss_item_sk;

-- ============================================================
-- MV: mv_009
-- Fact Table: store_sales
-- Tables: customer, item, promotion, store, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, item.i_item_sk=store_sales.ss_item_sk, promotion.p_promo_sk=store_sales.ss_promo_sk, ... (4 total)
-- QBs: q61.sql::qb::subquery:1::root.from.subquery1, q64.sql::qb::cte:cross_sales::root.with.cross_sales
-- Columns: 32 columns
-- ============================================================
CREATE VIEW mv_009 AS
SELECT
  i.i_item_sk,
  i.i_product_name,
  s.s_store_name,
  s.s_zip,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3,
  SUM(ss.ss_ext_sales_price) AS promotions
FROM customer AS c
INNER JOIN store_sales AS ss
  ON ss.ss_customer_sk = c.c_customer_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN promotion AS p
  ON p.p_promo_sk = ss.ss_promo_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  i.i_item_sk,
  i.i_product_name,
  s.s_store_name,
  s.s_zip;

-- ============================================================
-- MV: mv_010
-- Fact Table: store_sales
-- Tables: customer_address, date_dim, household_demographics, store, store_sales
-- Edges: customer_address.ca_address_sk=store_sales.ss_addr_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, household_demographics.hd_demo_sk=store_sales.ss_hdemo_sk, ... (4 total)
-- QBs: q46.sql::qb::subquery:1::root.from.subquery1, q68.sql::qb::subquery:1::root.from.subquery1
-- Columns: 22 columns
-- ============================================================
CREATE VIEW mv_010 AS
SELECT
  ca.ca_city,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number,
  SUM(ss.ss_coupon_amt) AS amt,
  SUM(ss.ss_net_profit) AS profit,
  SUM(ss.ss_ext_sales_price) AS extended_price,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax
FROM customer_address AS ca
INNER JOIN store_sales AS ss
  ON ss.ss_addr_sk = ca.ca_address_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = ss.ss_sold_date_sk
INNER JOIN household_demographics AS hd
  ON hd.hd_demo_sk = ss.ss_hdemo_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  ca.ca_city,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number;

-- ============================================================
-- MV: mv_011
-- Fact Table: store_sales
-- Tables: date_dim, item, promotion, store, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, promotion.p_promo_sk=store_sales.ss_promo_sk, ... (4 total)
-- QBs: q61.sql::qb::subquery:1::root.from.subquery1, q80.sql::qb::cte:ssr::root.with.ssr
-- Columns: 22 columns
-- ============================================================
CREATE VIEW mv_011 AS
SELECT
  s.s_store_id,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price
FROM date_dim AS d
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN promotion AS p
  ON p.p_promo_sk = ss.ss_promo_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  s.s_store_id;

-- ============================================================
-- MV: mv_012
-- Fact Table: store_sales
-- Tables: date_dim, item, store, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q19.sql::qb::main:0::root, q27.sql::qb::main:0::root, q34.sql::qb::subquery:1::root.from.subquery1, q43.sql::qb::main:0::root, q46.sql::qb::subquery:1::root.from.subquery1, ... (19 total)
-- Columns: 47 columns
-- ============================================================
CREATE VIEW mv_012 AS
SELECT
  d.d_moy,
  d.d_qoy,
  d.d_year,
  i.i_brand,
  i.i_brand_id,
  i.i_category,
  i.i_class,
  i.i_manager_id,
  i.i_manufact,
  i.i_manufact_id,
  s.s_city,
  s.s_company_name,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  s.s_store_sk,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  AVG(ss.ss_quantity) AS agg1,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4,
  SUM(ss.ss_coupon_amt) AS amt,
  SUM(ss.ss_net_profit) AS sum_ss__ss_net_profit,
  COUNT(*) AS cnt,
  SUM(ss.ss_sales_price) AS sum_ss__ss_sales_price,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax
FROM date_dim AS d
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  d.d_moy,
  d.d_qoy,
  d.d_year,
  i.i_brand,
  i.i_brand_id,
  i.i_category,
  i.i_class,
  i.i_manager_id,
  i.i_manufact,
  i.i_manufact_id,
  s.s_city,
  s.s_company_name,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  s.s_store_sk,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number;

-- ============================================================
-- MV: mv_013
-- Fact Table: store_sales
-- Tables: household_demographics, store, store_sales, time_dim
-- Edges: household_demographics.hd_demo_sk=store_sales.ss_hdemo_sk, store.s_store_sk=store_sales.ss_store_sk, store_sales.ss_sold_time_sk=time_dim.t_time_sk
-- QBs: q88.sql::qb::subquery:1::root.from.subquery1, q88.sql::qb::subquery:2::root.join.subquery2, q88.sql::qb::subquery:3::root.join.subquery3, q88.sql::qb::subquery:4::root.join.subquery4, q88.sql::qb::subquery:5::root.join.subquery5, ... (9 total)
-- Columns: 11 columns
-- ============================================================
CREATE VIEW mv_013 AS
SELECT
  COUNT(*) AS count_all
FROM household_demographics AS hd
INNER JOIN store_sales AS ss
  ON ss.ss_hdemo_sk = hd.hd_demo_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
INNER JOIN time_dim AS t
  ON t.t_time_sk = ss.ss_sold_time_sk;

-- ============================================================
-- MV: mv_014
-- Fact Table: store_sales
-- Tables: customer, item, store, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, item.i_item_sk=store_sales.ss_item_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q19.sql::qb::main:0::root, q24a.sql::qb::cte:ssales::root.with.ssales, q24b.sql::qb::cte:ssales::root.with.ssales, q61.sql::qb::subquery:1::root.from.subquery1, q61.sql::qb::subquery:2::root.join.subquery2, ... (6 total)
-- Columns: 41 columns
-- ============================================================
CREATE VIEW mv_014 AS
SELECT
  c.c_first_name,
  c.c_last_name,
  i.i_brand,
  i.i_brand_id,
  i.i_color,
  i.i_current_price,
  i.i_item_sk,
  i.i_manager_id,
  i.i_manufact,
  i.i_manufact_id,
  i.i_product_name,
  i.i_size,
  i.i_units,
  s.s_state,
  s.s_store_name,
  s.s_zip,
  SUM(ss.ss_net_paid) AS netpaid,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3
FROM customer AS c
INNER JOIN store_sales AS ss
  ON ss.ss_customer_sk = c.c_customer_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  c.c_first_name,
  c.c_last_name,
  i.i_brand,
  i.i_brand_id,
  i.i_color,
  i.i_current_price,
  i.i_item_sk,
  i.i_manager_id,
  i.i_manufact,
  i.i_manufact_id,
  i.i_product_name,
  i.i_size,
  i.i_units,
  s.s_state,
  s.s_store_name,
  s.s_zip;

-- ============================================================
-- MV: mv_015
-- Fact Table: store_sales
-- Tables: date_dim, item, store, store_sales
-- Edges: d1.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q17.sql::qb::main:0::root, q25.sql::qb::main:0::root, q29.sql::qb::main:0::root, q36.sql::qb::main:0::root, q64.sql::qb::cte:cross_sales::root.with.cross_sales, ... (6 total)
-- Columns: 34 columns
-- ============================================================
CREATE VIEW mv_015 AS
SELECT
  d1.d_year,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  i.i_product_name,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  s.s_zip,
  COUNT(ss.ss_quantity) AS store_sales_quantitycount,
  AVG(ss.ss_quantity) AS avg_ss__ss_quantity,
  STDDEV_SAMP(ss.ss_quantity) AS stddev_samp_ss__ss_quantity,
  SUM(ss.ss_net_profit) AS sum_ss__ss_net_profit,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3,
  SUM(ss.ss_quantity) AS store_sales_quantity
FROM date_dim AS d1
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d1.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  d1.d_year,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  i.i_product_name,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  s.s_zip;

-- ============================================================
-- MV: mv_016
-- Fact Table: store_sales
-- Tables: date_dim, household_demographics, store, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, household_demographics.hd_demo_sk=store_sales.ss_hdemo_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q34.sql::qb::subquery:1::root.from.subquery1, q46.sql::qb::subquery:1::root.from.subquery1, q68.sql::qb::subquery:1::root.from.subquery1, q73.sql::qb::subquery:1::root.from.subquery1, q79.sql::qb::subquery:1::root.from.subquery1
-- Columns: 23 columns
-- ============================================================
CREATE VIEW mv_016 AS
SELECT
  s.s_city,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number,
  COUNT(*) AS cnt,
  SUM(ss.ss_coupon_amt) AS amt,
  SUM(ss.ss_net_profit) AS profit,
  SUM(ss.ss_ext_sales_price) AS extended_price,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax
FROM date_dim AS d
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d.d_date_sk
INNER JOIN household_demographics AS hd
  ON hd.hd_demo_sk = ss.ss_hdemo_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  s.s_city,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number;

-- ============================================================
-- MV: mv_017
-- Fact Table: store_sales
-- Tables: customer_address, date_dim, item, store_sales
-- Edges: customer_address.ca_address_sk=store_sales.ss_addr_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk
-- QBs: q31.sql::qb::cte:ss::root.with.ss, q33.sql::qb::cte:ss::root.with.ss, q56.sql::qb::cte:ss::root.with.ss, q60.sql::qb::cte:ss::root.with.ss
-- Columns: 14 columns
-- ============================================================
CREATE VIEW mv_017 AS
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

-- ============================================================
-- MV: mv_018
-- Fact Table: store_sales
-- Tables: date_dim, item, promotion, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, promotion.p_promo_sk=store_sales.ss_promo_sk
-- QBs: q61.sql::qb::subquery:1::root.from.subquery1, q7.sql::qb::main:0::root, q80.sql::qb::cte:ssr::root.with.ssr
-- Columns: 26 columns
-- ============================================================
CREATE VIEW mv_018 AS
SELECT
  i.i_item_id,
  AVG(ss.ss_quantity) AS agg1,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price
FROM date_dim AS d
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN promotion AS p
  ON p.p_promo_sk = ss.ss_promo_sk
GROUP BY
  i.i_item_id;

-- ============================================================
-- MV: mv_019
-- Fact Table: store_sales
-- Tables: item, promotion, store, store_sales
-- Edges: item.i_item_sk=store_sales.ss_item_sk, promotion.p_promo_sk=store_sales.ss_promo_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q61.sql::qb::subquery:1::root.from.subquery1, q64.sql::qb::cte:cross_sales::root.with.cross_sales, q80.sql::qb::cte:ssr::root.with.ssr
-- Columns: 28 columns
-- ============================================================
CREATE VIEW mv_019 AS
SELECT
  i.i_item_sk,
  i.i_product_name,
  s.s_store_id,
  s.s_store_name,
  s.s_zip,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price
FROM item AS i
INNER JOIN store_sales AS ss
  ON ss.ss_item_sk = i.i_item_sk
INNER JOIN promotion AS p
  ON p.p_promo_sk = ss.ss_promo_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  i.i_item_sk,
  i.i_product_name,
  s.s_store_id,
  s.s_store_name,
  s.s_zip;

-- ============================================================
-- MV: mv_020
-- Fact Table: store_sales
-- Tables: customer_demographics, date_dim, item, store_sales
-- Edges: customer_demographics.cd_demo_sk=store_sales.ss_cdemo_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk
-- QBs: q27.sql::qb::main:0::root, q7.sql::qb::main:0::root
-- Columns: 17 columns
-- ============================================================
CREATE VIEW mv_020 AS
SELECT
  i.i_item_id,
  AVG(ss.ss_quantity) AS agg1,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4
FROM customer_demographics AS cd
INNER JOIN store_sales AS ss
  ON ss.ss_cdemo_sk = cd.cd_demo_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = ss.ss_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
GROUP BY
  i.i_item_id;

-- ============================================================
-- MV: mv_021
-- Fact Table: store_sales
-- Tables: date_dim, item, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk
-- QBs: q10.sql::qb::subquery:1::root.exists.exists1, q11.sql::qb::union_branch:1::root.with.year_total.union.left, q14a.sql::qb::union_branch:4::root.with.avg_sales.from.subquery2.union.left.union.left, q14a.sql::qb::union_branch:7::root.from.subquery3.union.left.union.left, q14b.sql::qb::subquery:3::root.from.subquery3, ... (42 total)
-- Columns: 41 columns
-- ============================================================
CREATE VIEW mv_021 AS
SELECT
  d.d_date,
  d.d_moy,
  d.d_qoy,
  d.d_week_seq,
  d.d_year,
  i.i_brand,
  i.i_brand_id,
  i.i_category,
  i.i_category_id,
  i.i_class,
  i.i_class_id,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  i.i_manager_id,
  i.i_manufact,
  i.i_manufact_id,
  ss.ss_customer_sk,
  ss.ss_item_sk,
  ss.ss_store_sk,
  SUM(ss.ss_sales_price) AS sum_ss__ss_sales_price,
  AVG(ss.ss_quantity) AS agg1,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  COUNT(*) AS count_all,
  SUM(ss.ss_net_paid) AS year_total
FROM date_dim AS d
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
GROUP BY
  d.d_date,
  d.d_moy,
  d.d_qoy,
  d.d_week_seq,
  d.d_year,
  i.i_brand,
  i.i_brand_id,
  i.i_category,
  i.i_category_id,
  i.i_class,
  i.i_class_id,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  i.i_manager_id,
  i.i_manufact,
  i.i_manufact_id,
  ss.ss_customer_sk,
  ss.ss_item_sk,
  ss.ss_store_sk;

-- ============================================================
-- MV: mv_022
-- Fact Table: store_sales
-- Tables: item, store, store_sales
-- Edges: item.i_item_sk=store_sales.ss_item_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q17.sql::qb::main:0::root, q19.sql::qb::main:0::root, q24a.sql::qb::cte:ssales::root.with.ssales, q24b.sql::qb::cte:ssales::root.with.ssales, q25.sql::qb::main:0::root, ... (37 total)
-- Columns: 51 columns
-- ============================================================
CREATE VIEW mv_022 AS
SELECT
  i.i_brand,
  i.i_brand_id,
  i.i_category,
  i.i_class,
  i.i_color,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  i.i_manager_id,
  i.i_manufact,
  i.i_manufact_id,
  i.i_product_name,
  i.i_size,
  i.i_units,
  s.s_city,
  s.s_company_id,
  s.s_company_name,
  s.s_county,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  s.s_store_sk,
  s.s_street_name,
  s.s_street_number,
  s.s_street_type,
  s.s_suite_number,
  s.s_zip,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number,
  SUM(ss.ss_net_paid) AS netpaid,
  COUNT(*) AS count_all,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax,
  SUM(ss.ss_net_profit) AS sum_ss__ss_net_profit,
  AVG(ss.ss_quantity) AS avg_ss__ss_quantity,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4,
  SUM(ss.ss_coupon_amt) AS sum_ss__ss_coupon_amt,
  SUM(ss.ss_quantity) AS store_sales_quantity,
  SUM(ss.ss_sales_price) AS sum_ss__ss_sales_price,
  COUNT(ss.ss_quantity) AS store_sales_quantitycount,
  STDDEV_SAMP(ss.ss_quantity) AS stddev_samp_ss__ss_quantity,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2
FROM item AS i
INNER JOIN store_sales AS ss
  ON ss.ss_item_sk = i.i_item_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  i.i_brand,
  i.i_brand_id,
  i.i_category,
  i.i_class,
  i.i_color,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  i.i_manager_id,
  i.i_manufact,
  i.i_manufact_id,
  i.i_product_name,
  i.i_size,
  i.i_units,
  s.s_city,
  s.s_company_id,
  s.s_company_name,
  s.s_county,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  s.s_store_sk,
  s.s_street_name,
  s.s_street_number,
  s.s_street_type,
  s.s_suite_number,
  s.s_zip,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number;

-- ============================================================
-- MV: mv_023
-- Fact Table: store_sales
-- Tables: household_demographics, store, store_sales
-- Edges: household_demographics.hd_demo_sk=store_sales.ss_hdemo_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q34.sql::qb::subquery:1::root.from.subquery1, q46.sql::qb::subquery:1::root.from.subquery1, q68.sql::qb::subquery:1::root.from.subquery1, q73.sql::qb::subquery:1::root.from.subquery1, q79.sql::qb::subquery:1::root.from.subquery1, ... (14 total)
-- Columns: 21 columns
-- ============================================================
CREATE VIEW mv_023 AS
SELECT
  s.s_city,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number,
  COUNT(*) AS count_all,
  SUM(ss.ss_coupon_amt) AS amt,
  SUM(ss.ss_net_profit) AS profit,
  SUM(ss.ss_ext_sales_price) AS extended_price,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax
FROM household_demographics AS hd
INNER JOIN store_sales AS ss
  ON ss.ss_hdemo_sk = hd.hd_demo_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  s.s_city,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number;

-- ============================================================
-- MV: mv_024
-- Fact Table: store_sales
-- Tables: customer, date_dim, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk
-- QBs: q11.sql::qb::union_branch:1::root.with.year_total.union.left, q19.sql::qb::main:0::root, q23a.sql::qb::subquery:1::root.with.max_store_sales.from.subquery1, q23b.sql::qb::subquery:1::root.with.max_store_sales.from.subquery1, q38.sql::qb::union_branch:1::root.from.subquery1.intersect.left.intersect.left, ... (10 total)
-- Columns: 26 columns
-- ============================================================
CREATE VIEW mv_024 AS
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

-- ============================================================
-- MV: mv_025
-- Fact Table: store_sales
-- Tables: date_dim, store, store_sales
-- Edges: d1.d_date_sk=store_sales.ss_sold_date_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q17.sql::qb::main:0::root, q25.sql::qb::main:0::root, q29.sql::qb::main:0::root, q36.sql::qb::main:0::root, q50.sql::qb::main:0::root, ... (7 total)
-- Columns: 32 columns
-- ============================================================
CREATE VIEW mv_025 AS
SELECT
  d1.d_year,
  s.s_city,
  s.s_company_id,
  s.s_county,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  s.s_street_name,
  s.s_street_number,
  s.s_street_type,
  s.s_suite_number,
  s.s_zip,
  COUNT(ss.ss_quantity) AS store_sales_quantitycount,
  AVG(ss.ss_quantity) AS avg_ss__ss_quantity,
  STDDEV_SAMP(ss.ss_quantity) AS stddev_samp_ss__ss_quantity,
  SUM(ss.ss_net_profit) AS sum_ss__ss_net_profit,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3,
  SUM(ss.ss_quantity) AS store_sales_quantity
FROM date_dim AS d1
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d1.d_date_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  d1.d_year,
  s.s_city,
  s.s_company_id,
  s.s_county,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  s.s_street_name,
  s.s_street_number,
  s.s_street_type,
  s.s_suite_number,
  s.s_zip;

-- ============================================================
-- MV: mv_026
-- Fact Table: store_sales
-- Tables: customer_address, date_dim, store_sales
-- Edges: customer_address.ca_address_sk=store_sales.ss_addr_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk
-- QBs: q31.sql::qb::cte:ss::root.with.ss, q33.sql::qb::cte:ss::root.with.ss, q46.sql::qb::subquery:1::root.from.subquery1, q56.sql::qb::cte:ss::root.with.ss, q60.sql::qb::cte:ss::root.with.ss, ... (6 total)
-- Columns: 22 columns
-- ============================================================
CREATE VIEW mv_026 AS
SELECT
  ca.ca_city,
  ca.ca_county,
  d.d_qoy,
  d.d_year,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_ticket_number,
  SUM(ss.ss_coupon_amt) AS amt,
  SUM(ss.ss_net_profit) AS profit,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax
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

-- ============================================================
-- MV: mv_027
-- Fact Table: store_sales
-- Tables: date_dim, item, store_sales
-- Edges: d1.d_date_sk=store_sales.ss_sold_date_sk, iss.i_item_sk=store_sales.ss_item_sk
-- QBs: q14a.sql::qb::union_branch:1::root.with.cross_items.join.subquery1.intersect.left.intersect.left, q14b.sql::qb::union_branch:1::root.with.cross_items.join.subquery1.intersect.left.intersect.left, q17.sql::qb::main:0::root, q25.sql::qb::main:0::root, q29.sql::qb::main:0::root
-- Columns: 17 columns
-- ============================================================
CREATE VIEW mv_027 AS
SELECT
  iss.i_item_desc,
  iss.i_item_id,
  COUNT(ss.ss_quantity) AS store_sales_quantitycount,
  AVG(ss.ss_quantity) AS avg_ss__ss_quantity,
  STDDEV_SAMP(ss.ss_quantity) AS stddev_samp_ss__ss_quantity,
  SUM(ss.ss_net_profit) AS store_sales_profit,
  SUM(ss.ss_quantity) AS store_sales_quantity
FROM date_dim AS d1
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d1.d_date_sk
INNER JOIN item AS iss
  ON iss.i_item_sk = ss.ss_item_sk
GROUP BY
  iss.i_item_desc,
  iss.i_item_id;

-- ============================================================
-- MV: mv_028
-- Fact Table: store_sales
-- Tables: item, promotion, store_sales
-- Edges: item.i_item_sk=store_sales.ss_item_sk, promotion.p_promo_sk=store_sales.ss_promo_sk
-- QBs: q61.sql::qb::subquery:1::root.from.subquery1, q64.sql::qb::cte:cross_sales::root.with.cross_sales, q7.sql::qb::main:0::root, q80.sql::qb::cte:ssr::root.with.ssr
-- Columns: 27 columns
-- ============================================================
CREATE VIEW mv_028 AS
SELECT
  i.i_item_id,
  i.i_item_sk,
  i.i_product_name,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3,
  AVG(ss.ss_quantity) AS agg1,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price
FROM item AS i
INNER JOIN store_sales AS ss
  ON ss.ss_item_sk = i.i_item_sk
INNER JOIN promotion AS p
  ON p.p_promo_sk = ss.ss_promo_sk
GROUP BY
  i.i_item_id,
  i.i_item_sk,
  i.i_product_name;

-- ============================================================
-- MV: mv_029
-- Fact Table: store_sales
-- Tables: date_dim, item, store_sales
-- Edges: dt.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk
-- QBs: q3.sql::qb::main:0::root, q42.sql::qb::main:0::root, q52.sql::qb::main:0::root
-- Columns: 13 columns
-- ============================================================
CREATE VIEW mv_029 AS
SELECT
  dt.d_year,
  i.i_brand,
  i.i_brand_id,
  i.i_category,
  i.i_category_id,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price
FROM date_dim AS dt
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = dt.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
GROUP BY
  dt.d_year,
  i.i_brand,
  i.i_brand_id,
  i.i_category,
  i.i_category_id;

-- ============================================================
-- MV: mv_030
-- Fact Table: store_sales
-- Tables: item, store_sales
-- Edges: item.i_item_sk=store_sales.ss_item_sk
-- QBs: q10.sql::qb::subquery:1::root.exists.exists1, q11.sql::qb::union_branch:1::root.with.year_total.union.left, q14a.sql::qb::union_branch:4::root.with.avg_sales.from.subquery2.union.left.union.left, q14a.sql::qb::union_branch:7::root.from.subquery3.union.left.union.left, q14b.sql::qb::subquery:3::root.from.subquery3, ... (64 total)
-- Columns: 39 columns
-- ============================================================
CREATE VIEW mv_030 AS
SELECT
  i.i_brand,
  i.i_brand_id,
  i.i_category,
  i.i_category_id,
  i.i_class,
  i.i_class_id,
  i.i_color,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  i.i_manager_id,
  i.i_manufact,
  i.i_manufact_id,
  i.i_product_name,
  i.i_size,
  i.i_units,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_item_sk,
  ss.ss_store_sk,
  ss.ss_ticket_number,
  SUM(ss.ss_net_paid) AS sum_ss__ss_net_paid,
  SUM(ss.ss_sales_price) AS sum_ss__ss_sales_price,
  SUM(ss.ss_net_profit) AS sum_ss__ss_net_profit,
  AVG(ss.ss_quantity) AS avg_ss__ss_quantity,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4,
  SUM(ss.ss_coupon_amt) AS sum_ss__ss_coupon_amt,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  SUM(ss.ss_quantity) AS sum_ss__ss_quantity,
  COUNT(*) AS count_all,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax,
  SUM(ss.ss_wholesale_cost) AS sum_ss__ss_wholesale_cost,
  COUNT(ss.ss_quantity) AS store_sales_quantitycount,
  STDDEV_SAMP(ss.ss_quantity) AS stddev_samp_ss__ss_quantity,
  SUM(ss.ss_list_price) AS s2
FROM item AS i
INNER JOIN store_sales AS ss
  ON ss.ss_item_sk = i.i_item_sk
GROUP BY
  i.i_brand,
  i.i_brand_id,
  i.i_category,
  i.i_category_id,
  i.i_class,
  i.i_class_id,
  i.i_color,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  i.i_manager_id,
  i.i_manufact,
  i.i_manufact_id,
  i.i_product_name,
  i.i_size,
  i.i_units,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_item_sk,
  ss.ss_store_sk,
  ss.ss_ticket_number;

-- ============================================================
-- MV: mv_031
-- Fact Table: store_sales
-- Tables: date_dim, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk
-- QBs: q10.sql::qb::subquery:1::root.exists.exists1, q11.sql::qb::union_branch:1::root.with.year_total.union.left, q14a.sql::qb::union_branch:4::root.with.avg_sales.from.subquery2.union.left.union.left, q14a.sql::qb::union_branch:7::root.from.subquery3.union.left.union.left, q14b.sql::qb::subquery:3::root.from.subquery3, ... (53 total)
-- Columns: 32 columns
-- ============================================================
CREATE VIEW mv_031 AS
SELECT
  d.d_date,
  d.d_moy,
  d.d_qoy,
  d.d_week_seq,
  d.d_year,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_item_sk,
  ss.ss_store_sk,
  ss.ss_ticket_number,
  SUM(ss.ss_sales_price) AS sum_ss__ss_sales_price,
  AVG(ss.ss_quantity) AS agg1,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4,
  SUM(ss.ss_coupon_amt) AS amt,
  SUM(ss.ss_net_profit) AS sum_ss__ss_net_profit,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  COUNT(*) AS count_all,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax,
  SUM(ss.ss_net_paid) AS year_total,
  SUM(ss.ss_quantity) AS ss_qty,
  SUM(ss.ss_wholesale_cost) AS ss_wc
FROM date_dim AS d
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d.d_date_sk
GROUP BY
  d.d_date,
  d.d_moy,
  d.d_qoy,
  d.d_week_seq,
  d.d_year,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_item_sk,
  ss.ss_store_sk,
  ss.ss_ticket_number;

-- ============================================================
-- MV: mv_032
-- Fact Table: store_sales
-- Tables: customer, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk
-- QBs: q11.sql::qb::union_branch:1::root.with.year_total.union.left, q19.sql::qb::main:0::root, q23a.sql::qb::cte:best_ss_customer::root.with.best_ss_customer, q23a.sql::qb::subquery:1::root.with.max_store_sales.from.subquery1, q23b.sql::qb::cte:best_ss_customer::root.with.best_ss_customer, ... (15 total)
-- Columns: 32 columns
-- ============================================================
CREATE VIEW mv_032 AS
SELECT
  c.c_birth_country,
  c.c_customer_id,
  c.c_customer_sk,
  c.c_email_address,
  c.c_first_name,
  c.c_last_name,
  c.c_login,
  c.c_preferred_cust_flag,
  SUM(ss.ss_net_paid) AS sum_ss__ss_net_paid,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3
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

-- ============================================================
-- MV: mv_033
-- Fact Table: store_sales
-- Tables: date_dim, store_sales
-- Edges: d1.d_date_sk=store_sales.ss_sold_date_sk
-- QBs: q14a.sql::qb::union_branch:1::root.with.cross_items.join.subquery1.intersect.left.intersect.left, q14b.sql::qb::union_branch:1::root.with.cross_items.join.subquery1.intersect.left.intersect.left, q17.sql::qb::main:0::root, q25.sql::qb::main:0::root, q29.sql::qb::main:0::root, ... (9 total)
-- Columns: 20 columns
-- ============================================================
CREATE VIEW mv_033 AS
SELECT
  d1.d_year,
  COUNT(ss.ss_quantity) AS store_sales_quantitycount,
  AVG(ss.ss_quantity) AS avg_ss__ss_quantity,
  STDDEV_SAMP(ss.ss_quantity) AS stddev_samp_ss__ss_quantity,
  SUM(ss.ss_net_profit) AS sum_ss__ss_net_profit,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3,
  SUM(ss.ss_quantity) AS store_sales_quantity
FROM date_dim AS d1
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d1.d_date_sk
GROUP BY
  d1.d_year;

-- ============================================================
-- MV: mv_034
-- Fact Table: store_returns
-- Tables: date_dim, item, store_returns
-- Edges: date_dim.d_date_sk=store_returns.sr_returned_date_sk, item.i_item_sk=store_returns.sr_item_sk
-- QBs: q1.sql::qb::cte:customer_total_return::root.with.customer_total_return, q77.sql::qb::cte:sr::root.with.sr, q83.sql::qb::cte:sr_items::root.with.sr_items
-- Columns: 12 columns
-- ============================================================
CREATE VIEW mv_034 AS
SELECT
  i.i_item_id,
  sr.sr_customer_sk,
  sr.sr_store_sk,
  SUM(sr.sr_return_amt) AS sum_sr__sr_return_amt,
  SUM(sr.sr_net_loss) AS profit_loss,
  SUM(sr.sr_return_quantity) AS sr_item_qty
FROM date_dim AS d
INNER JOIN store_returns AS sr
  ON sr.sr_returned_date_sk = d.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = sr.sr_item_sk
GROUP BY
  i.i_item_id,
  sr.sr_customer_sk,
  sr.sr_store_sk;

-- ============================================================
-- MV: mv_035
-- Fact Table: customer
-- Tables: customer, customer_address, customer_demographics
-- Edges: c.c_current_addr_sk=ca.ca_address_sk, c.c_current_cdemo_sk=customer_demographics.cd_demo_sk
-- QBs: q10.sql::qb::main:0::root, q35.sql::qb::main:0::root, q69.sql::qb::main:0::root
-- Columns: 14 columns
-- ============================================================
CREATE VIEW mv_035 AS
SELECT
  ca.ca_state,
  cd.cd_credit_rating,
  cd.cd_dep_college_count,
  cd.cd_dep_count,
  cd.cd_dep_employed_count,
  cd.cd_education_status,
  cd.cd_gender,
  cd.cd_marital_status,
  cd.cd_purchase_estimate,
  COUNT(*) AS count_all,
  MIN(cd.cd_dep_count) AS min_cd__cd_dep_count,
  MAX(cd.cd_dep_count) AS max_cd__cd_dep_count,
  AVG(cd.cd_dep_count) AS avg_cd__cd_dep_count,
  MIN(cd.cd_dep_employed_count) AS min_cd__cd_dep_employed_count,
  MAX(cd.cd_dep_employed_count) AS max_cd__cd_dep_employed_count,
  AVG(cd.cd_dep_employed_count) AS avg_cd__cd_dep_employed_count,
  MIN(cd.cd_dep_college_count) AS min_cd__cd_dep_college_count,
  MAX(cd.cd_dep_college_count) AS max_cd__cd_dep_college_count,
  AVG(cd.cd_dep_college_count) AS avg_cd__cd_dep_college_count
FROM customer AS c
INNER JOIN customer_address AS ca
  ON ca.ca_address_sk = c.c_current_addr_sk
INNER JOIN customer_demographics AS cd
  ON cd.cd_demo_sk = c.c_current_cdemo_sk
GROUP BY
  ca.ca_state,
  cd.cd_credit_rating,
  cd.cd_dep_college_count,
  cd.cd_dep_count,
  cd.cd_dep_employed_count,
  cd.cd_education_status,
  cd.cd_gender,
  cd.cd_marital_status,
  cd.cd_purchase_estimate;

-- ============================================================
-- MV: mv_036
-- Fact Table: customer
-- Tables: customer, customer_address
-- Edges: customer.c_current_addr_sk=customer_address.ca_address_sk
-- QBs: q30.sql::qb::main:0::root, q8.sql::qb::subquery:3::root.join.subquery1.from.subquery2.intersect.right.from.subquery3, q81.sql::qb::main:0::root
-- Columns: 26 columns
-- ============================================================
CREATE VIEW mv_036 AS
SELECT
  ca.ca_zip,
  COUNT(*) AS cnt
FROM customer AS c
INNER JOIN customer_address AS ca
  ON ca.ca_address_sk = c.c_current_addr_sk
GROUP BY
  ca.ca_zip;

-- ============================================================
-- MV: mv_037
-- Fact Table: customer
-- Tables: customer, customer_address
-- Edges: current_addr.ca_address_sk=customer.c_current_addr_sk
-- QBs: q46.sql::qb::main:0::root, q68.sql::qb::main:0::root
-- Columns: 6 columns
-- ============================================================
CREATE VIEW mv_037 AS
SELECT
  current_addr.ca_address_sk,
  current_addr.ca_city,
  c.c_current_addr_sk,
  c.c_customer_sk,
  c.c_first_name,
  c.c_last_name
FROM customer_address AS current_addr
INNER JOIN customer AS c
  ON c.c_current_addr_sk = current_addr.ca_address_sk;

-- ============================================================
-- MV: mv_038
-- Fact Table: web_sales
-- Tables: customer_address, date_dim, item, web_sales
-- Edges: customer_address.ca_address_sk=web_sales.ws_bill_addr_sk, date_dim.d_date_sk=web_sales.ws_sold_date_sk, item.i_item_sk=web_sales.ws_item_sk
-- QBs: q31.sql::qb::cte:ws::root.with.ws, q33.sql::qb::cte:ws::root.with.ws, q56.sql::qb::cte:ws::root.with.ws, q60.sql::qb::cte:ws::root.with.ws
-- Columns: 14 columns
-- ============================================================
CREATE VIEW mv_038 AS
SELECT
  ca.ca_county,
  d.d_qoy,
  d.d_year,
  i.i_item_id,
  i.i_manufact_id,
  SUM(ws.ws_ext_sales_price) AS sum_ws__ws_ext_sales_price
FROM customer_address AS ca
INNER JOIN web_sales AS ws
  ON ws.ws_bill_addr_sk = ca.ca_address_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = ws.ws_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ws.ws_item_sk
GROUP BY
  ca.ca_county,
  d.d_qoy,
  d.d_year,
  i.i_item_id,
  i.i_manufact_id;

-- ============================================================
-- MV: mv_039
-- Fact Table: web_sales
-- Tables: customer_address, date_dim, web_sales, web_site
-- Edges: customer_address.ca_address_sk=ws1.ws_ship_addr_sk, date_dim.d_date_sk=ws1.ws_ship_date_sk, web_site.web_site_sk=ws1.ws_web_site_sk
-- QBs: q94.sql::qb::main:0::root, q95.sql::qb::main:0::root
-- Columns: 12 columns
-- ============================================================
CREATE VIEW mv_039 AS
SELECT
  SUM(ws1.ws_ext_ship_cost) AS `total shipping cost `,
  SUM(ws1.ws_net_profit) AS `total net profit `
FROM customer_address AS ca
INNER JOIN web_sales AS ws1
  ON ws1.ws_ship_addr_sk = ca.ca_address_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = ws1.ws_ship_date_sk
INNER JOIN web_site AS web
  ON web.web_site_sk = ws1.ws_web_site_sk;

-- ============================================================
-- MV: mv_040
-- Fact Table: web_sales
-- Tables: household_demographics, time_dim, web_page, web_sales
-- Edges: household_demographics.hd_demo_sk=web_sales.ws_ship_hdemo_sk, time_dim.t_time_sk=web_sales.ws_sold_time_sk, web_page.wp_web_page_sk=web_sales.ws_web_page_sk
-- QBs: q90.sql::qb::subquery:1::root.from.subquery1, q90.sql::qb::subquery:2::root.join.subquery2
-- Columns: 9 columns
-- ============================================================
CREATE VIEW mv_040 AS
SELECT
  COUNT(*) AS count_all
FROM household_demographics AS hd
INNER JOIN web_sales AS ws
  ON ws.ws_ship_hdemo_sk = hd.hd_demo_sk
INNER JOIN time_dim AS t
  ON t.t_time_sk = ws.ws_sold_time_sk
INNER JOIN web_page AS wp
  ON wp.wp_web_page_sk = ws.ws_web_page_sk;

-- ============================================================
-- MV: mv_041
-- Fact Table: web_sales
-- Tables: date_dim, item, web_sales
-- Edges: date_dim.d_date_sk=web_sales.ws_sold_date_sk, item.i_item_sk=web_sales.ws_item_sk
-- QBs: q10.sql::qb::subquery:2::root.exists.exists2, q11.sql::qb::union_branch:2::root.with.year_total.union.right, q12.sql::qb::main:0::root, q14a.sql::qb::union_branch:6::root.with.avg_sales.from.subquery2.union.right, q14a.sql::qb::union_branch:9::root.from.subquery3.union.right, ... (25 total)
-- Columns: 31 columns
-- ============================================================
CREATE VIEW mv_041 AS
SELECT
  d.d_date,
  d.d_year,
  i.i_brand_id,
  i.i_category,
  i.i_category_id,
  i.i_class,
  i.i_class_id,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_manufact_id,
  ws.ws_item_sk,
  SUM(ws.ws_ext_sales_price) AS sum_ws__ws_ext_sales_price,
  SUM(ws.ws_ext_discount_amt) AS `Excess Discount Amount `,
  SUM(ws.ws_sales_price) AS sum_ws__ws_sales_price,
  SUM(ws.ws_net_paid) AS year_total,
  COUNT(*) AS number_sales,
  AVG(ws.ws_ext_discount_amt) AS avg_ws__ws_ext_discount_amt
FROM date_dim AS d
INNER JOIN web_sales AS ws
  ON ws.ws_sold_date_sk = d.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ws.ws_item_sk
GROUP BY
  d.d_date,
  d.d_year,
  i.i_brand_id,
  i.i_category,
  i.i_category_id,
  i.i_class,
  i.i_class_id,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_manufact_id,
  ws.ws_item_sk;

-- ============================================================
-- MV: mv_042
-- Fact Table: web_sales
-- Tables: customer, date_dim, web_sales
-- Edges: customer.c_customer_sk=web_sales.ws_bill_customer_sk, date_dim.d_date_sk=web_sales.ws_sold_date_sk
-- QBs: q11.sql::qb::union_branch:2::root.with.year_total.union.right, q23b.sql::qb::union_branch:2::root.from.subquery3.union.right, q38.sql::qb::union_branch:3::root.from.subquery1.intersect.right, q4.sql::qb::union_branch:3::root.with.year_total.union.right, q45.sql::qb::main:0::root, ... (7 total)
-- Columns: 26 columns
-- ============================================================
CREATE VIEW mv_042 AS
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

-- ============================================================
-- MV: mv_043
-- Fact Table: web_sales
-- Tables: date_dim, item, web_sales
-- Edges: d3.d_date_sk=web_sales.ws_sold_date_sk, iws.i_item_sk=web_sales.ws_item_sk
-- QBs: q14a.sql::qb::union_branch:3::root.with.cross_items.join.subquery1.intersect.right, q14b.sql::qb::union_branch:3::root.with.cross_items.join.subquery1.intersect.right
-- Columns: 8 columns
-- ============================================================
CREATE VIEW mv_043 AS
SELECT
  d3.d_date_sk,
  d3.d_year,
  iws.i_brand_id,
  iws.i_category_id,
  iws.i_class_id,
  iws.i_item_sk,
  ws.ws_item_sk,
  ws.ws_sold_date_sk
FROM date_dim AS d3
INNER JOIN web_sales AS ws
  ON ws.ws_sold_date_sk = d3.d_date_sk
INNER JOIN item AS iws
  ON iws.i_item_sk = ws.ws_item_sk;

-- ============================================================
-- MV: mv_044
-- Fact Table: web_sales
-- Tables: ship_mode, warehouse, web_sales
-- Edges: ship_mode.sm_ship_mode_sk=web_sales.ws_ship_mode_sk, warehouse.w_warehouse_sk=web_sales.ws_warehouse_sk
-- QBs: q62.sql::qb::main:0::root, q66.sql::qb::union_branch:1::root.from.subquery1.union.left
-- Columns: 19 columns
-- ============================================================
CREATE VIEW mv_044 AS
SELECT
  sm.sm_type,
  w.w_city,
  w.w_country,
  w.w_county,
  w.w_state,
  w.w_warehouse_name,
  w.w_warehouse_sq_ft
FROM ship_mode AS sm
INNER JOIN web_sales AS ws
  ON ws.ws_ship_mode_sk = sm.sm_ship_mode_sk
INNER JOIN warehouse AS w
  ON w.w_warehouse_sk = ws.ws_warehouse_sk
GROUP BY
  sm.sm_type,
  w.w_city,
  w.w_country,
  w.w_county,
  w.w_state,
  w.w_warehouse_name,
  w.w_warehouse_sq_ft;

-- ============================================================
-- MV: mv_045
-- Fact Table: web_sales
-- Tables: date_dim, web_sales
-- Edges: date_dim.d_date_sk=web_sales.ws_sold_date_sk
-- QBs: q10.sql::qb::subquery:2::root.exists.exists2, q11.sql::qb::union_branch:2::root.with.year_total.union.right, q12.sql::qb::main:0::root, q14a.sql::qb::union_branch:6::root.with.avg_sales.from.subquery2.union.right, q14a.sql::qb::union_branch:9::root.from.subquery3.union.right, ... (28 total)
-- Columns: 25 columns
-- ============================================================
CREATE VIEW mv_045 AS
SELECT
  d.d_date,
  d.d_qoy,
  d.d_year,
  ws.ws_item_sk,
  SUM(ws.ws_ext_sales_price) AS sum_ws__ws_ext_sales_price,
  SUM(ws.ws_net_profit) AS profit,
  SUM(ws.ws_ext_discount_amt) AS `Excess Discount Amount `,
  SUM(ws.ws_sales_price) AS sum_ws__ws_sales_price,
  SUM(ws.ws_net_paid) AS year_total,
  COUNT(*) AS number_sales,
  AVG(ws.ws_ext_discount_amt) AS avg_ws__ws_ext_discount_amt
FROM date_dim AS d
INNER JOIN web_sales AS ws
  ON ws.ws_sold_date_sk = d.d_date_sk
GROUP BY
  d.d_date,
  d.d_qoy,
  d.d_year,
  ws.ws_item_sk;

-- ============================================================
-- MV: mv_046
-- Fact Table: web_sales
-- Tables: item, web_sales
-- Edges: item.i_item_sk=web_sales.ws_item_sk
-- QBs: q10.sql::qb::subquery:2::root.exists.exists2, q11.sql::qb::union_branch:2::root.with.year_total.union.right, q12.sql::qb::main:0::root, q14a.sql::qb::union_branch:6::root.with.avg_sales.from.subquery2.union.right, q14a.sql::qb::union_branch:9::root.from.subquery3.union.right, ... (27 total)
-- Columns: 25 columns
-- ============================================================
CREATE VIEW mv_046 AS
SELECT
  i.i_brand_id,
  i.i_category,
  i.i_category_id,
  i.i_class,
  i.i_class_id,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_manufact_id,
  ws.ws_item_sk,
  SUM(ws.ws_net_paid) AS sum_ws__ws_net_paid,
  SUM(ws.ws_ext_sales_price) AS sum_ws__ws_ext_sales_price,
  SUM(ws.ws_ext_discount_amt) AS `Excess Discount Amount `,
  SUM(ws.ws_sales_price) AS sum_ws__ws_sales_price,
  COUNT(*) AS number_sales,
  AVG(ws.ws_ext_discount_amt) AS avg_ws__ws_ext_discount_amt
FROM item AS i
INNER JOIN web_sales AS ws
  ON ws.ws_item_sk = i.i_item_sk
GROUP BY
  i.i_brand_id,
  i.i_category,
  i.i_category_id,
  i.i_class,
  i.i_class_id,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_manufact_id,
  ws.ws_item_sk;

-- ============================================================
-- MV: mv_047
-- Fact Table: web_sales
-- Tables: time_dim, web_sales
-- Edges: time_dim.t_time_sk=web_sales.ws_sold_time_sk
-- QBs: q66.sql::qb::union_branch:1::root.from.subquery1.union.left, q90.sql::qb::subquery:1::root.from.subquery1, q90.sql::qb::subquery:2::root.join.subquery2
-- Columns: 12 columns
-- ============================================================
CREATE VIEW mv_047 AS
SELECT
  COUNT(*) AS count_all
FROM time_dim AS t
INNER JOIN web_sales AS ws
  ON ws.ws_sold_time_sk = t.t_time_sk;

-- ============================================================
-- MV: mv_048
-- Fact Table: web_sales
-- Tables: web_page, web_sales
-- Edges: web_page.wp_web_page_sk=web_sales.ws_web_page_sk
-- QBs: q77.sql::qb::cte:ws::root.with.ws, q90.sql::qb::subquery:1::root.from.subquery1, q90.sql::qb::subquery:2::root.join.subquery2
-- Columns: 8 columns
-- ============================================================
CREATE VIEW mv_048 AS
SELECT
  wp.wp_web_page_sk,
  COUNT(*) AS count_all,
  SUM(ws.ws_ext_sales_price) AS sales,
  SUM(ws.ws_net_profit) AS profit
FROM web_page AS wp
INNER JOIN web_sales AS ws
  ON ws.ws_web_page_sk = wp.wp_web_page_sk
GROUP BY
  wp.wp_web_page_sk;

-- ============================================================
-- MV: mv_049
-- Fact Table: catalog_sales
-- Tables: catalog_returns, catalog_sales, date_dim, item
-- Edges: catalog_sales.cs_item_sk=catalog_returns.cr_item_sk, catalog_sales.cs_item_sk=item.i_item_sk, catalog_sales.cs_order_number=catalog_returns.cr_order_number, ... (4 total)
-- QBs: q75.sql::qb::union_branch:1::root.with.all_sales.from.subquery1.union.left.union.left, q78.sql::qb::cte:cs::root.with.cs
-- Columns: 20 columns
-- ============================================================
CREATE VIEW mv_049 AS
SELECT
  cs.cs_bill_customer_sk,
  cs.cs_item_sk,
  d.d_year,
  SUM(cs.cs_quantity) AS cs_qty,
  SUM(cs.cs_wholesale_cost) AS cs_wc,
  SUM(cs.cs_sales_price) AS cs_sp
FROM catalog_sales AS cs
LEFT JOIN catalog_returns AS cr
  ON cr.cr_item_sk = cs.cs_item_sk AND cr.cr_order_number = cs.cs_order_number
INNER JOIN date_dim AS d
  ON d.d_date_sk = cs.cs_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = cs.cs_item_sk
GROUP BY
  cs.cs_bill_customer_sk,
  cs.cs_item_sk,
  d.d_year;

-- ============================================================
-- MV: mv_050
-- Fact Table: catalog_sales
-- Tables: catalog_sales, customer_address, date_dim, item
-- Edges: catalog_sales.cs_bill_addr_sk=customer_address.ca_address_sk, catalog_sales.cs_item_sk=item.i_item_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk
-- QBs: q33.sql::qb::cte:cs::root.with.cs, q56.sql::qb::cte:cs::root.with.cs, q60.sql::qb::cte:cs::root.with.cs
-- Columns: 12 columns
-- ============================================================
CREATE VIEW mv_050 AS
SELECT
  i.i_item_id,
  i.i_manufact_id,
  SUM(cs.cs_ext_sales_price) AS total_sales
FROM catalog_sales AS cs
INNER JOIN customer_address AS ca
  ON ca.ca_address_sk = cs.cs_bill_addr_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = cs.cs_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = cs.cs_item_sk
GROUP BY
  i.i_item_id,
  i.i_manufact_id;

-- ============================================================
-- MV: mv_051
-- Fact Table: catalog_sales
-- Tables: catalog_sales, customer, customer_address, date_dim
-- Edges: catalog_sales.cs_bill_customer_sk=customer.c_customer_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk, customer.c_current_addr_sk=customer_address.ca_address_sk
-- QBs: q15.sql::qb::main:0::root, q18.sql::qb::main:0::root
-- Columns: 22 columns
-- ============================================================
CREATE VIEW mv_051 AS
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

-- ============================================================
-- MV: mv_052
-- Fact Table: catalog_sales
-- Tables: catalog_sales, date_dim, item
-- Edges: catalog_sales.cs_item_sk=item.i_item_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk
-- QBs: q10.sql::qb::subquery:3::root.exists.exists3, q14a.sql::qb::union_branch:5::root.with.avg_sales.from.subquery2.union.left.union.right, q14a.sql::qb::union_branch:8::root.from.subquery3.union.left.union.right, q14b.sql::qb::union_branch:5::root.with.avg_sales.from.subquery2.union.left.union.right, q18.sql::qb::main:0::root, ... (22 total)
-- Columns: 35 columns
-- ============================================================
CREATE VIEW mv_052 AS
SELECT
  cs.cs_bill_customer_sk,
  cs.cs_call_center_sk,
  cs.cs_item_sk,
  d.d_moy,
  d.d_year,
  i.i_brand,
  i.i_brand_id,
  i.i_category,
  i.i_category_id,
  i.i_class,
  i.i_class_id,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_manufact_id,
  AVG(cs.cs_quantity) AS agg1,
  AVG(cs.cs_list_price) AS agg2,
  AVG(cs.cs_coupon_amt) AS agg3,
  AVG(cs.cs_sales_price) AS agg4,
  SUM(cs.cs_ext_sales_price) AS sum_cs__cs_ext_sales_price,
  AVG(cs.cs_ext_discount_amt) AS avg_cs__cs_ext_discount_amt,
  SUM(cs.cs_ext_discount_amt) AS `excess discount amount`,
  SUM(cs.cs_sales_price) AS sum_cs__cs_sales_price,
  SUM(cs.cs_net_profit) AS profit,
  COUNT(*) AS number_sales
FROM catalog_sales AS cs
INNER JOIN date_dim AS d
  ON d.d_date_sk = cs.cs_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = cs.cs_item_sk
GROUP BY
  cs.cs_bill_customer_sk,
  cs.cs_call_center_sk,
  cs.cs_item_sk,
  d.d_moy,
  d.d_year,
  i.i_brand,
  i.i_brand_id,
  i.i_category,
  i.i_category_id,
  i.i_class,
  i.i_class_id,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_manufact_id;

-- ============================================================
-- MV: mv_053
-- Fact Table: catalog_sales
-- Tables: catalog_sales, customer, date_dim
-- Edges: catalog_sales.cs_bill_customer_sk=customer.c_customer_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk
-- QBs: q15.sql::qb::main:0::root, q18.sql::qb::main:0::root, q23b.sql::qb::union_branch:1::root.from.subquery3.union.left, q38.sql::qb::union_branch:2::root.from.subquery1.intersect.left.intersect.right, q4.sql::qb::union_branch:2::root.with.year_total.union.left.union.right, ... (6 total)
-- Columns: 31 columns
-- ============================================================
CREATE VIEW mv_053 AS
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

-- ============================================================
-- MV: mv_054
-- Fact Table: catalog_sales
-- Tables: catalog_sales, date_dim, item
-- Edges: catalog_sales.cs_item_sk=ics.i_item_sk, catalog_sales.cs_sold_date_sk=d2.d_date_sk
-- QBs: q14a.sql::qb::union_branch:2::root.with.cross_items.join.subquery1.intersect.left.intersect.right, q14b.sql::qb::union_branch:2::root.with.cross_items.join.subquery1.intersect.left.intersect.right
-- Columns: 8 columns
-- ============================================================
CREATE VIEW mv_054 AS
SELECT
  cs.cs_item_sk,
  cs.cs_sold_date_sk,
  d2.d_date_sk,
  d2.d_year,
  ics.i_brand_id,
  ics.i_category_id,
  ics.i_class_id,
  ics.i_item_sk
FROM catalog_sales AS cs
INNER JOIN date_dim AS d2
  ON d2.d_date_sk = cs.cs_sold_date_sk
INNER JOIN item AS ics
  ON ics.i_item_sk = cs.cs_item_sk;

-- ============================================================
-- MV: mv_055
-- Fact Table: catalog_sales
-- Tables: catalog_sales, ship_mode, warehouse
-- Edges: catalog_sales.cs_ship_mode_sk=ship_mode.sm_ship_mode_sk, catalog_sales.cs_warehouse_sk=warehouse.w_warehouse_sk
-- QBs: q66.sql::qb::union_branch:2::root.from.subquery1.union.right, q99.sql::qb::main:0::root
-- Columns: 19 columns
-- ============================================================
CREATE VIEW mv_055 AS
SELECT
  sm.sm_type,
  w.w_city,
  w.w_country,
  w.w_county,
  w.w_state,
  w.w_warehouse_name,
  w.w_warehouse_sq_ft
FROM catalog_sales AS cs
INNER JOIN ship_mode AS sm
  ON sm.sm_ship_mode_sk = cs.cs_ship_mode_sk
INNER JOIN warehouse AS w
  ON w.w_warehouse_sk = cs.cs_warehouse_sk
GROUP BY
  sm.sm_type,
  w.w_city,
  w.w_country,
  w.w_county,
  w.w_state,
  w.w_warehouse_name,
  w.w_warehouse_sq_ft;

-- ============================================================
-- MV: mv_056
-- Fact Table: catalog_sales
-- Tables: catalog_sales, date_dim
-- Edges: catalog_sales.cs_sold_date_sk=date_dim.d_date_sk
-- QBs: q10.sql::qb::subquery:3::root.exists.exists3, q14a.sql::qb::union_branch:5::root.with.avg_sales.from.subquery2.union.left.union.right, q14a.sql::qb::union_branch:8::root.from.subquery3.union.left.union.right, q14b.sql::qb::union_branch:5::root.with.avg_sales.from.subquery2.union.left.union.right, q15.sql::qb::main:0::root, ... (29 total)
-- Columns: 30 columns
-- ============================================================
CREATE VIEW mv_056 AS
SELECT
  cs.cs_bill_customer_sk,
  cs.cs_call_center_sk,
  cs.cs_item_sk,
  d.d_moy,
  d.d_year,
  AVG(cs.cs_quantity) AS agg1,
  AVG(cs.cs_list_price) AS agg2,
  AVG(cs.cs_coupon_amt) AS agg3,
  AVG(cs.cs_sales_price) AS agg4,
  SUM(cs.cs_ext_sales_price) AS sum_cs__cs_ext_sales_price,
  AVG(cs.cs_ext_discount_amt) AS avg_cs__cs_ext_discount_amt,
  SUM(cs.cs_ext_discount_amt) AS `excess discount amount`,
  SUM(cs.cs_sales_price) AS sum_cs__cs_sales_price,
  SUM(cs.cs_quantity) AS cs_qty,
  SUM(cs.cs_wholesale_cost) AS cs_wc,
  SUM(cs.cs_net_profit) AS profit,
  COUNT(*) AS number_sales
FROM catalog_sales AS cs
INNER JOIN date_dim AS d
  ON d.d_date_sk = cs.cs_sold_date_sk
GROUP BY
  cs.cs_bill_customer_sk,
  cs.cs_call_center_sk,
  cs.cs_item_sk,
  d.d_moy,
  d.d_year;

-- ============================================================
-- MV: mv_057
-- Fact Table: catalog_sales
-- Tables: call_center, catalog_sales
-- Edges: call_center.cc_call_center_sk=catalog_sales.cs_call_center_sk
-- QBs: q57.sql::qb::cte:v1::root.with.v1, q99.sql::qb::main:0::root
-- Columns: 9 columns
-- ============================================================
CREATE VIEW mv_057 AS
SELECT
  cc.cc_name,
  SUM(cs.cs_sales_price) AS sum_cs__cs_sales_price
FROM call_center AS cc
INNER JOIN catalog_sales AS cs
  ON cs.cs_call_center_sk = cc.cc_call_center_sk
GROUP BY
  cc.cc_name;

-- ============================================================
-- MV: mv_058
-- Fact Table: inventory
-- Tables: catalog_sales, date_dim, inventory, item, warehouse
-- Edges: catalog_sales.cs_item_sk=item.i_item_sk, date_dim.d_date_sk=inventory.inv_date_sk, inventory.inv_item_sk=item.i_item_sk, ... (4 total)
-- QBs: q21.sql::qb::subquery:1::root.from.subquery1, q22.sql::qb::main:0::root, q37.sql::qb::main:0::root, q39a.sql::qb::subquery:1::root.with.inv.from.subquery1, q39b.sql::qb::subquery:1::root.with.inv.from.subquery1, ... (6 total)
-- Columns: 21 columns
-- ============================================================
CREATE VIEW mv_058 AS
SELECT
  d.d_moy,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  w.w_warehouse_name,
  w.w_warehouse_sk,
  STDDEV_SAMP(inv.inv_quantity_on_hand) AS stdev,
  AVG(inv.inv_quantity_on_hand) AS avg_inv__inv_quantity_on_hand
FROM catalog_sales AS cs
INNER JOIN item AS i
  ON i.i_item_sk = cs.cs_item_sk
INNER JOIN inventory AS inv
  ON inv.inv_item_sk = i.i_item_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = inv.inv_date_sk
INNER JOIN warehouse AS w
  ON w.w_warehouse_sk = inv.inv_warehouse_sk
GROUP BY
  d.d_moy,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  w.w_warehouse_name,
  w.w_warehouse_sk;

-- ============================================================
-- MV: mv_059
-- Fact Table: inventory
-- Tables: catalog_sales, date_dim, inventory, item, store_sales
-- Edges: catalog_sales.cs_item_sk=item.i_item_sk, date_dim.d_date_sk=inventory.inv_date_sk, inventory.inv_item_sk=item.i_item_sk, ... (4 total)
-- QBs: q21.sql::qb::subquery:1::root.from.subquery1, q22.sql::qb::main:0::root, q37.sql::qb::main:0::root, q39a.sql::qb::subquery:1::root.with.inv.from.subquery1, q39b.sql::qb::subquery:1::root.with.inv.from.subquery1, ... (6 total)
-- Columns: 20 columns
-- ============================================================
CREATE VIEW mv_059 AS
SELECT
  d.d_moy,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  STDDEV_SAMP(inv.inv_quantity_on_hand) AS stdev,
  AVG(inv.inv_quantity_on_hand) AS avg_inv__inv_quantity_on_hand
FROM catalog_sales AS cs
INNER JOIN item AS i
  ON i.i_item_sk = cs.cs_item_sk
INNER JOIN inventory AS inv
  ON inv.inv_item_sk = i.i_item_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = inv.inv_date_sk
INNER JOIN store_sales AS ss
  ON ss.ss_item_sk = i.i_item_sk
GROUP BY
  d.d_moy,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk;

-- ============================================================
-- MV: mv_060
-- Fact Table: inventory
-- Tables: date_dim, inventory, item, store_sales, warehouse
-- Edges: date_dim.d_date_sk=inventory.inv_date_sk, inventory.inv_item_sk=item.i_item_sk, inventory.inv_warehouse_sk=warehouse.w_warehouse_sk, ... (4 total)
-- QBs: q21.sql::qb::subquery:1::root.from.subquery1, q22.sql::qb::main:0::root, q37.sql::qb::main:0::root, q39a.sql::qb::subquery:1::root.with.inv.from.subquery1, q39b.sql::qb::subquery:1::root.with.inv.from.subquery1, ... (6 total)
-- Columns: 21 columns
-- ============================================================
CREATE VIEW mv_060 AS
SELECT
  d.d_moy,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  w.w_warehouse_name,
  w.w_warehouse_sk,
  STDDEV_SAMP(inv.inv_quantity_on_hand) AS stdev,
  AVG(inv.inv_quantity_on_hand) AS avg_inv__inv_quantity_on_hand
FROM date_dim AS d
INNER JOIN inventory AS inv
  ON inv.inv_date_sk = d.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = inv.inv_item_sk
INNER JOIN store_sales AS ss
  ON ss.ss_item_sk = i.i_item_sk
INNER JOIN warehouse AS w
  ON w.w_warehouse_sk = inv.inv_warehouse_sk
GROUP BY
  d.d_moy,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  w.w_warehouse_name,
  w.w_warehouse_sk;

-- ============================================================
-- MV: mv_061
-- Fact Table: web_returns
-- Tables: date_dim, web_returns, web_sales
-- Edges: date_dim.d_date_sk=web_sales.ws_sold_date_sk, web_sales.ws_item_sk=web_returns.wr_item_sk, web_sales.ws_order_number=web_returns.wr_order_number
-- QBs: q78.sql::qb::cte:ws::root.with.ws, q80.sql::qb::cte:wsr::root.with.wsr
-- Columns: 18 columns
-- ============================================================
CREATE VIEW mv_061 AS
SELECT
  d.d_year,
  ws.ws_bill_customer_sk,
  ws.ws_item_sk,
  SUM(ws.ws_quantity) AS ws_qty,
  SUM(ws.ws_wholesale_cost) AS ws_wc,
  SUM(ws.ws_sales_price) AS ws_sp,
  SUM(ws.ws_ext_sales_price) AS sales
FROM date_dim AS d
INNER JOIN web_sales AS ws
  ON ws.ws_sold_date_sk = d.d_date_sk
LEFT JOIN web_returns AS wr
  ON wr.wr_item_sk = ws.ws_item_sk AND wr.wr_order_number = ws.ws_order_number
GROUP BY
  d.d_year,
  ws.ws_bill_customer_sk,
  ws.ws_item_sk;

-- ============================================================
-- MV: mv_062
-- Fact Table: web_returns
-- Tables: date_dim, item, web_returns
-- Edges: date_dim.d_date_sk=web_returns.wr_returned_date_sk, item.i_item_sk=web_returns.wr_item_sk
-- QBs: q30.sql::qb::cte:customer_total_return::root.with.customer_total_return, q77.sql::qb::cte:wr::root.with.wr, q83.sql::qb::cte:wr_items::root.with.wr_items
-- Columns: 13 columns
-- ============================================================
CREATE VIEW mv_062 AS
SELECT
  i.i_item_id,
  wr.wr_returning_customer_sk,
  SUM(wr.wr_return_amt) AS sum_wr__wr_return_amt,
  SUM(wr.wr_net_loss) AS profit_loss,
  SUM(wr.wr_return_quantity) AS wr_item_qty
FROM date_dim AS d
INNER JOIN web_returns AS wr
  ON wr.wr_returned_date_sk = d.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = wr.wr_item_sk
GROUP BY
  i.i_item_id,
  wr.wr_returning_customer_sk;

-- ============================================================
-- MV: mv_063
-- Fact Table: web_returns
-- Tables: date_dim, web_sales
-- Edges: date_dim.d_date_sk=web_sales.ws_sold_date_sk
-- QBs: q78.sql::qb::cte:ws::root.with.ws, q80.sql::qb::cte:wsr::root.with.wsr, q85.sql::qb::main:0::root
-- Columns: 15 columns
-- ============================================================
CREATE VIEW mv_063 AS
SELECT
  d.d_year,
  ws.ws_bill_customer_sk,
  ws.ws_item_sk,
  SUM(ws.ws_quantity) AS ws_qty,
  SUM(ws.ws_wholesale_cost) AS ws_wc,
  SUM(ws.ws_sales_price) AS ws_sp,
  SUM(ws.ws_ext_sales_price) AS sales,
  AVG(ws.ws_quantity) AS avg_ws__ws_quantity
FROM date_dim AS d
INNER JOIN web_sales AS ws
  ON ws.ws_sold_date_sk = d.d_date_sk
GROUP BY
  d.d_year,
  ws.ws_bill_customer_sk,
  ws.ws_item_sk;

-- ============================================================
-- MV: mv_064
-- Fact Table: catalog_returns
-- Tables: catalog_returns, catalog_sales, date_dim, item
-- Edges: catalog_sales.cs_item_sk=catalog_returns.cr_item_sk, catalog_sales.cs_item_sk=item.i_item_sk, catalog_sales.cs_order_number=catalog_returns.cr_order_number, ... (4 total)
-- QBs: q40.sql::qb::main:0::root, q80.sql::qb::cte:csr::root.with.csr
-- Columns: 19 columns
-- ============================================================
CREATE VIEW mv_064 AS
SELECT
  i.i_item_id,
  SUM(cs.cs_ext_sales_price) AS sales
FROM catalog_sales AS cs
LEFT JOIN catalog_returns AS cr
  ON cr.cr_item_sk = cs.cs_item_sk AND cr.cr_order_number = cs.cs_order_number
INNER JOIN date_dim AS d
  ON d.d_date_sk = cs.cs_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = cs.cs_item_sk
GROUP BY
  i.i_item_id;

-- ============================================================
-- MV: mv_065
-- Fact Table: catalog_returns
-- Tables: catalog_returns, date_dim, item
-- Edges: catalog_returns.cr_item_sk=item.i_item_sk, catalog_returns.cr_returned_date_sk=date_dim.d_date_sk
-- QBs: q77.sql::qb::cte:cr::root.with.cr, q81.sql::qb::cte:customer_total_return::root.with.customer_total_return, q83.sql::qb::cte:cr_items::root.with.cr_items
-- Columns: 13 columns
-- ============================================================
CREATE VIEW mv_065 AS
SELECT
  cr.cr_returning_customer_sk,
  i.i_item_id,
  SUM(cr.cr_return_amt_inc_tax) AS ctr_total_return,
  SUM(cr.cr_return_quantity) AS cr_item_qty,
  SUM(cr.cr_return_amount) AS returns,
  SUM(cr.cr_net_loss) AS profit_loss
FROM catalog_returns AS cr
INNER JOIN date_dim AS d
  ON d.d_date_sk = cr.cr_returned_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = cr.cr_item_sk
GROUP BY
  cr.cr_returning_customer_sk,
  i.i_item_id;

-- ============================================================
-- MV: mv_066
-- Fact Table: catalog_returns
-- Tables: catalog_returns, date_dim
-- Edges: catalog_returns.cr_returned_date_sk=date_dim.d_date_sk
-- QBs: q77.sql::qb::cte:cr::root.with.cr, q81.sql::qb::cte:customer_total_return::root.with.customer_total_return, q83.sql::qb::cte:cr_items::root.with.cr_items, q91.sql::qb::main:0::root
-- Columns: 13 columns
-- ============================================================
CREATE VIEW mv_066 AS
SELECT
  cr.cr_returning_customer_sk,
  SUM(cr.cr_return_amt_inc_tax) AS ctr_total_return,
  SUM(cr.cr_net_loss) AS sum_cr__cr_net_loss,
  SUM(cr.cr_return_quantity) AS cr_item_qty,
  SUM(cr.cr_return_amount) AS returns
FROM catalog_returns AS cr
INNER JOIN date_dim AS d
  ON d.d_date_sk = cr.cr_returned_date_sk
GROUP BY
  cr.cr_returning_customer_sk;

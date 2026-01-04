-- ECSE Candidate Materialized Views
-- Generated: 2026-01-04T14:40:52.297195
-- Dialect: spark
-- Total MVs: 66

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
    customer.c_first_name,
    customer.c_last_name,
    customer_address.ca_state,
    item.i_color,
    item.i_current_price,
    item.i_manager_id,
    item.i_size,
    item.i_units,
    store.s_state,
    store.s_store_name,
    SUM(store_sales.ss_net_paid) AS netpaid
FROM customer
INNER JOIN store_sales
    ON store_sales.ss_customer_sk = customer.c_customer_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
INNER JOIN customer_address
    ON customer_address.ca_zip = store.s_zip
INNER JOIN store_returns
    ON store_returns.sr_ticket_number = store_sales.ss_ticket_number
GROUP BY customer.c_first_name, customer.c_last_name, customer_address.ca_state, item.i_color, item.i_current_price, item.i_manager_id, item.i_size, item.i_units, store.s_state, store.s_store_name;

-- ============================================================
-- MV: mv_002
-- Fact Table: store_sales
-- Tables: customer, customer_address, date_dim, item, store, store_sales
-- Edges: customer.c_current_addr_sk=customer_address.ca_address_sk, customer.c_customer_sk=store_sales.ss_customer_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, ... (5 total)
-- QBs: q19.sql::qb::main:0::root, q61.sql::qb::subquery:1::root.from.subquery1, q61.sql::qb::subquery:2::root.join.subquery2
-- Columns: 24 columns
-- ============================================================
CREATE VIEW mv_002 AS
SELECT
    item.i_brand,
    item.i_brand_id,
    item.i_manufact,
    item.i_manufact_id,
    SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price
FROM customer
INNER JOIN customer_address
    ON customer_address.ca_address_sk = customer.c_current_addr_sk
INNER JOIN store_sales
    ON store_sales.ss_customer_sk = customer.c_customer_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
GROUP BY item.i_brand, item.i_brand_id, item.i_manufact, item.i_manufact_id;

-- ============================================================
-- MV: mv_003
-- Fact Table: store_sales
-- Tables: customer, item, store, store_returns, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, item.i_item_sk=store_sales.ss_item_sk, store.s_store_sk=store_sales.ss_store_sk, ... (5 total)
-- QBs: q24a.sql::qb::cte:ssales::root.with.ssales, q24b.sql::qb::cte:ssales::root.with.ssales, q64.sql::qb::cte:cross_sales::root.with.cross_sales
-- Columns: 36 columns
-- ============================================================
CREATE VIEW mv_003 AS
SELECT
    customer.c_first_name,
    customer.c_last_name,
    item.i_color,
    item.i_current_price,
    item.i_item_sk,
    item.i_manager_id,
    item.i_product_name,
    item.i_size,
    item.i_units,
    store.s_state,
    store.s_store_name,
    store.s_zip,
    SUM(store_sales.ss_net_paid) AS netpaid,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3
FROM customer
INNER JOIN store_sales
    ON store_sales.ss_customer_sk = customer.c_customer_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
INNER JOIN store_returns
    ON store_returns.sr_item_sk = store_sales.ss_item_sk
GROUP BY customer.c_first_name, customer.c_last_name, item.i_color, item.i_current_price, item.i_item_sk, item.i_manager_id, item.i_product_name, item.i_size, item.i_units, store.s_state, store.s_store_name, store.s_zip;

-- ============================================================
-- MV: mv_004
-- Fact Table: store_sales
-- Tables: customer, date_dim, item, promotion, store, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, ... (5 total)
-- QBs: q61.sql::qb::subquery:1::root.from.subquery1, q64.sql::qb::cte:cross_sales::root.with.cross_sales
-- Columns: 35 columns
-- ============================================================
CREATE VIEW mv_004 AS
SELECT
    date_dim.d_year,
    item.i_item_sk,
    item.i_product_name,
    store.s_store_name,
    store.s_zip,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3,
    SUM(store_sales.ss_ext_sales_price) AS promotions
FROM customer
INNER JOIN store_sales
    ON store_sales.ss_customer_sk = customer.c_customer_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
INNER JOIN promotion
    ON promotion.p_promo_sk = store_sales.ss_promo_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
GROUP BY date_dim.d_year, item.i_item_sk, item.i_product_name, store.s_store_name, store.s_zip;

-- ============================================================
-- MV: mv_005
-- Fact Table: store_sales
-- Tables: customer, date_dim, item, store, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, ... (4 total)
-- QBs: q19.sql::qb::main:0::root, q61.sql::qb::subquery:1::root.from.subquery1, q61.sql::qb::subquery:2::root.join.subquery2, q64.sql::qb::cte:cross_sales::root.with.cross_sales
-- Columns: 36 columns
-- ============================================================
CREATE VIEW mv_005 AS
SELECT
    date_dim.d_year,
    item.i_brand,
    item.i_brand_id,
    item.i_item_sk,
    item.i_manufact,
    item.i_manufact_id,
    item.i_product_name,
    store.s_store_name,
    store.s_zip,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3,
    SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price
FROM customer
INNER JOIN store_sales
    ON store_sales.ss_customer_sk = customer.c_customer_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
GROUP BY date_dim.d_year, item.i_brand, item.i_brand_id, item.i_item_sk, item.i_manufact, item.i_manufact_id, item.i_product_name, store.s_store_name, store.s_zip;

-- ============================================================
-- MV: mv_006
-- Fact Table: store_sales
-- Tables: date_dim, item, store_returns, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, store_sales.ss_item_sk=store_returns.sr_item_sk, ... (4 total)
-- QBs: q49.sql::qb::subquery:6::root.union.right.from.subquery5.from.subquery6, q75.sql::qb::union_branch:2::root.with.all_sales.from.subquery1.union.left.union.right, q78.sql::qb::cte:ss::root.with.ss, q80.sql::qb::cte:ssr::root.with.ssr
-- Columns: 28 columns
-- ============================================================
CREATE VIEW mv_006 AS
SELECT
    date_dim.d_year,
    store_sales.ss_customer_sk,
    store_sales.ss_item_sk,
    SUM(store_sales.ss_ext_sales_price) AS sales,
    SUM(store_sales.ss_quantity) AS ss_qty,
    SUM(store_sales.ss_wholesale_cost) AS ss_wc,
    SUM(store_sales.ss_sales_price) AS ss_sp
FROM date_dim
INNER JOIN store_sales
    ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
LEFT JOIN store_returns
    ON store_returns.sr_ticket_number = store_sales.ss_ticket_number
GROUP BY date_dim.d_year, store_sales.ss_customer_sk, store_sales.ss_item_sk;

-- ============================================================
-- MV: mv_007
-- Fact Table: store_sales
-- Tables: customer_address, date_dim, household_demographics, store, store_sales
-- Edges: customer_address.ca_address_sk=store_sales.ss_addr_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, household_demographics.hd_demo_sk=store_sales.ss_hdemo_sk, ... (4 total)
-- QBs: q46.sql::qb::subquery:1::root.from.subquery1, q64.sql::qb::cte:cross_sales::root.with.cross_sales, q68.sql::qb::subquery:1::root.from.subquery1
-- Columns: 33 columns
-- ============================================================
CREATE VIEW mv_007 AS
SELECT
    customer_address.ca_city,
    customer_address.ca_street_name,
    customer_address.ca_street_number,
    customer_address.ca_zip,
    date_dim.d_year,
    store.s_store_name,
    store.s_zip,
    store_sales.ss_addr_sk,
    store_sales.ss_customer_sk,
    store_sales.ss_ticket_number,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS sum_store_sales__ss_coupon_amt,
    SUM(store_sales.ss_ext_sales_price) AS extended_price,
    SUM(store_sales.ss_ext_list_price) AS list_price,
    SUM(store_sales.ss_ext_tax) AS extended_tax,
    SUM(store_sales.ss_net_profit) AS profit
FROM customer_address
INNER JOIN store_sales
    ON store_sales.ss_addr_sk = customer_address.ca_address_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
INNER JOIN household_demographics
    ON household_demographics.hd_demo_sk = store_sales.ss_hdemo_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
GROUP BY customer_address.ca_city, customer_address.ca_street_name, customer_address.ca_street_number, customer_address.ca_zip, date_dim.d_year, store.s_store_name, store.s_zip, store_sales.ss_addr_sk, store_sales.ss_customer_sk, store_sales.ss_ticket_number;

-- ============================================================
-- MV: mv_008
-- Fact Table: store_sales
-- Tables: date_dim, item, promotion, store, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, promotion.p_promo_sk=store_sales.ss_promo_sk, ... (4 total)
-- QBs: q61.sql::qb::subquery:1::root.from.subquery1, q64.sql::qb::cte:cross_sales::root.with.cross_sales, q80.sql::qb::cte:ssr::root.with.ssr
-- Columns: 32 columns
-- ============================================================
CREATE VIEW mv_008 AS
SELECT
    date_dim.d_year,
    item.i_item_sk,
    item.i_product_name,
    store.s_store_id,
    store.s_store_name,
    store.s_zip,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3,
    SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price
FROM date_dim
INNER JOIN store_sales
    ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
INNER JOIN promotion
    ON promotion.p_promo_sk = store_sales.ss_promo_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
GROUP BY date_dim.d_year, item.i_item_sk, item.i_product_name, store.s_store_id, store.s_store_name, store.s_zip;

-- ============================================================
-- MV: mv_009
-- Fact Table: store_sales
-- Tables: customer, customer_address, date_dim, item, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, customer_address.ca_address_sk=customer.c_current_addr_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, ... (4 total)
-- QBs: q6.sql::qb::main:0::root, q64.sql::qb::cte:cross_sales::root.with.cross_sales
-- Columns: 31 columns
-- ============================================================
CREATE VIEW mv_009 AS
SELECT
    customer_address.ca_city,
    customer_address.ca_state,
    customer_address.ca_street_name,
    customer_address.ca_street_number,
    customer_address.ca_zip,
    date_dim.d_year,
    item.i_item_sk,
    item.i_product_name,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3
FROM customer
INNER JOIN customer_address
    ON customer_address.ca_address_sk = customer.c_current_addr_sk
INNER JOIN store_sales
    ON store_sales.ss_customer_sk = customer.c_customer_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
GROUP BY customer_address.ca_city, customer_address.ca_state, customer_address.ca_street_name, customer_address.ca_street_number, customer_address.ca_zip, date_dim.d_year, item.i_item_sk, item.i_product_name;

-- ============================================================
-- MV: mv_010
-- Fact Table: store_sales
-- Tables: customer_demographics, date_dim, item, promotion, store_sales
-- Edges: customer_demographics.cd_demo_sk=store_sales.ss_cdemo_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, ... (4 total)
-- QBs: q64.sql::qb::cte:cross_sales::root.with.cross_sales, q7.sql::qb::main:0::root
-- Columns: 28 columns
-- ============================================================
CREATE VIEW mv_010 AS
SELECT
    date_dim.d_year,
    item.i_item_id,
    item.i_item_sk,
    item.i_product_name,
    AVG(store_sales.ss_quantity) AS agg1,
    AVG(store_sales.ss_list_price) AS agg2,
    AVG(store_sales.ss_coupon_amt) AS agg3,
    AVG(store_sales.ss_sales_price) AS agg4,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3
FROM customer_demographics
INNER JOIN store_sales
    ON store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
INNER JOIN promotion
    ON promotion.p_promo_sk = store_sales.ss_promo_sk
GROUP BY date_dim.d_year, item.i_item_id, item.i_item_sk, item.i_product_name;

-- ============================================================
-- MV: mv_011
-- Fact Table: store_sales
-- Tables: customer_demographics, date_dim, item, store, store_sales
-- Edges: customer_demographics.cd_demo_sk=store_sales.ss_cdemo_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, ... (4 total)
-- QBs: q27.sql::qb::main:0::root, q64.sql::qb::cte:cross_sales::root.with.cross_sales
-- Columns: 29 columns
-- ============================================================
CREATE VIEW mv_011 AS
SELECT
    date_dim.d_year,
    item.i_item_sk,
    item.i_product_name,
    store.s_store_name,
    store.s_zip,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3,
    AVG(store_sales.ss_quantity) AS agg1,
    AVG(store_sales.ss_list_price) AS agg2,
    AVG(store_sales.ss_coupon_amt) AS agg3,
    AVG(store_sales.ss_sales_price) AS agg4
FROM customer_demographics
INNER JOIN store_sales
    ON store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
GROUP BY date_dim.d_year, item.i_item_sk, item.i_product_name, store.s_store_name, store.s_zip;

-- ============================================================
-- MV: mv_012
-- Fact Table: store_sales
-- Tables: date_dim, store, store_returns, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, store.s_store_sk=store_sales.ss_store_sk, store_returns.sr_item_sk=store_sales.ss_item_sk, ... (4 total)
-- QBs: q50.sql::qb::main:0::root, q64.sql::qb::cte:cross_sales::root.with.cross_sales
-- Columns: 30 columns
-- ============================================================
CREATE VIEW mv_012 AS
SELECT
    date_dim.d_year,
    store.s_city,
    store.s_company_id,
    store.s_county,
    store.s_state,
    store.s_store_name,
    store.s_street_name,
    store.s_street_number,
    store.s_street_type,
    store.s_suite_number,
    store.s_zip,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3
FROM date_dim
INNER JOIN store_sales
    ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
INNER JOIN store_returns
    ON store_returns.sr_ticket_number = store_sales.ss_ticket_number
GROUP BY date_dim.d_year, store.s_city, store.s_company_id, store.s_county, store.s_state, store.s_store_name, store.s_street_name, store.s_street_number, store.s_street_type, store.s_suite_number, store.s_zip;

-- ============================================================
-- MV: mv_013
-- Fact Table: store_sales
-- Tables: date_dim, item, store, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q19.sql::qb::main:0::root, q27.sql::qb::main:0::root, q34.sql::qb::subquery:1::root.from.subquery1, q36.sql::qb::main:0::root, q43.sql::qb::main:0::root, ... (22 total)
-- Columns: 49 columns
-- ============================================================
CREATE VIEW mv_013 AS
SELECT
    date_dim.d_moy,
    date_dim.d_qoy,
    date_dim.d_year,
    item.i_brand,
    item.i_brand_id,
    item.i_category,
    item.i_class,
    item.i_item_sk,
    item.i_manager_id,
    item.i_manufact,
    item.i_manufact_id,
    item.i_product_name,
    store.s_city,
    store.s_company_name,
    store.s_state,
    store.s_store_id,
    store.s_store_name,
    store.s_store_sk,
    store.s_zip,
    store_sales.ss_addr_sk,
    store_sales.ss_customer_sk,
    store_sales.ss_ticket_number,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_sales_price) AS sum_store_sales__ss_sales_price,
    SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price,
    SUM(store_sales.ss_net_profit) AS sum_store_sales__ss_net_profit,
    SUM(store_sales.ss_coupon_amt) AS sum_store_sales__ss_coupon_amt,
    AVG(store_sales.ss_quantity) AS agg1,
    AVG(store_sales.ss_list_price) AS agg2,
    AVG(store_sales.ss_coupon_amt) AS agg3,
    AVG(store_sales.ss_sales_price) AS agg4,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_ext_list_price) AS list_price,
    SUM(store_sales.ss_ext_tax) AS extended_tax
FROM date_dim
INNER JOIN store_sales
    ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
GROUP BY date_dim.d_moy, date_dim.d_qoy, date_dim.d_year, item.i_brand, item.i_brand_id, item.i_category, item.i_class, item.i_item_sk, item.i_manager_id, item.i_manufact, item.i_manufact_id, item.i_product_name, store.s_city, store.s_company_name, store.s_state, store.s_store_id, store.s_store_name, store.s_store_sk, store.s_zip, store_sales.ss_addr_sk, store_sales.ss_customer_sk, store_sales.ss_ticket_number;

-- ============================================================
-- MV: mv_014
-- Fact Table: store_sales
-- Tables: customer, date_dim, item, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk
-- QBs: q11.sql::qb::union_branch:1::root.with.year_total.union.left, q19.sql::qb::main:0::root, q23a.sql::qb::subquery:1::root.with.max_store_sales.from.subquery1, q23b.sql::qb::subquery:1::root.with.max_store_sales.from.subquery1, q38.sql::qb::union_branch:1::root.from.subquery1.intersect.left.intersect.left, ... (12 total)
-- Columns: 47 columns
-- ============================================================
CREATE VIEW mv_014 AS
SELECT
    customer.c_birth_country,
    customer.c_customer_id,
    customer.c_customer_sk,
    customer.c_email_address,
    customer.c_first_name,
    customer.c_last_name,
    customer.c_login,
    customer.c_preferred_cust_flag,
    date_dim.d_year,
    item.i_brand,
    item.i_brand_id,
    item.i_item_sk,
    item.i_manufact,
    item.i_manufact_id,
    item.i_product_name,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3,
    SUM(store_sales.ss_net_paid) AS year_total
FROM customer
INNER JOIN store_sales
    ON store_sales.ss_customer_sk = customer.c_customer_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
GROUP BY customer.c_birth_country, customer.c_customer_id, customer.c_customer_sk, customer.c_email_address, customer.c_first_name, customer.c_last_name, customer.c_login, customer.c_preferred_cust_flag, date_dim.d_year, item.i_brand, item.i_brand_id, item.i_item_sk, item.i_manufact, item.i_manufact_id, item.i_product_name;

-- ============================================================
-- MV: mv_015
-- Fact Table: store_sales
-- Tables: household_demographics, store, store_sales, time_dim
-- Edges: household_demographics.hd_demo_sk=store_sales.ss_hdemo_sk, store.s_store_sk=store_sales.ss_store_sk, store_sales.ss_sold_time_sk=time_dim.t_time_sk
-- QBs: q88.sql::qb::subquery:1::root.from.subquery1, q88.sql::qb::subquery:2::root.join.subquery2, q88.sql::qb::subquery:3::root.join.subquery3, q88.sql::qb::subquery:4::root.join.subquery4, q88.sql::qb::subquery:5::root.join.subquery5, ... (9 total)
-- Columns: 11 columns
-- ============================================================
CREATE VIEW mv_015 AS
SELECT
    COUNT(*) AS count_all
FROM household_demographics
INNER JOIN store_sales
    ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
INNER JOIN time_dim
    ON time_dim.t_time_sk = store_sales.ss_sold_time_sk;

-- ============================================================
-- MV: mv_016
-- Fact Table: store_sales
-- Tables: customer, item, store, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, item.i_item_sk=store_sales.ss_item_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q19.sql::qb::main:0::root, q24a.sql::qb::cte:ssales::root.with.ssales, q24b.sql::qb::cte:ssales::root.with.ssales, q61.sql::qb::subquery:1::root.from.subquery1, q61.sql::qb::subquery:2::root.join.subquery2, ... (6 total)
-- Columns: 41 columns
-- ============================================================
CREATE VIEW mv_016 AS
SELECT
    customer.c_first_name,
    customer.c_last_name,
    item.i_brand,
    item.i_brand_id,
    item.i_color,
    item.i_current_price,
    item.i_item_sk,
    item.i_manager_id,
    item.i_manufact,
    item.i_manufact_id,
    item.i_product_name,
    item.i_size,
    item.i_units,
    store.s_state,
    store.s_store_name,
    store.s_zip,
    SUM(store_sales.ss_net_paid) AS netpaid,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3,
    SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price
FROM customer
INNER JOIN store_sales
    ON store_sales.ss_customer_sk = customer.c_customer_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
GROUP BY customer.c_first_name, customer.c_last_name, item.i_brand, item.i_brand_id, item.i_color, item.i_current_price, item.i_item_sk, item.i_manager_id, item.i_manufact, item.i_manufact_id, item.i_product_name, item.i_size, item.i_units, store.s_state, store.s_store_name, store.s_zip;

-- ============================================================
-- MV: mv_017
-- Fact Table: store_sales
-- Tables: date_dim, household_demographics, store, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, household_demographics.hd_demo_sk=store_sales.ss_hdemo_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q34.sql::qb::subquery:1::root.from.subquery1, q46.sql::qb::subquery:1::root.from.subquery1, q64.sql::qb::cte:cross_sales::root.with.cross_sales, q68.sql::qb::subquery:1::root.from.subquery1, q73.sql::qb::subquery:1::root.from.subquery1, ... (6 total)
-- Columns: 31 columns
-- ============================================================
CREATE VIEW mv_017 AS
SELECT
    date_dim.d_year,
    store.s_city,
    store.s_store_name,
    store.s_zip,
    store_sales.ss_addr_sk,
    store_sales.ss_customer_sk,
    store_sales.ss_ticket_number,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS sum_store_sales__ss_coupon_amt,
    SUM(store_sales.ss_ext_sales_price) AS extended_price,
    SUM(store_sales.ss_ext_list_price) AS list_price,
    SUM(store_sales.ss_ext_tax) AS extended_tax,
    SUM(store_sales.ss_net_profit) AS profit
FROM date_dim
INNER JOIN store_sales
    ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
INNER JOIN household_demographics
    ON household_demographics.hd_demo_sk = store_sales.ss_hdemo_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
GROUP BY date_dim.d_year, store.s_city, store.s_store_name, store.s_zip, store_sales.ss_addr_sk, store_sales.ss_customer_sk, store_sales.ss_ticket_number;

-- ============================================================
-- MV: mv_018
-- Fact Table: store_sales
-- Tables: customer_address, date_dim, item, store_sales
-- Edges: customer_address.ca_address_sk=store_sales.ss_addr_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk
-- QBs: q31.sql::qb::cte:ss::root.with.ss, q33.sql::qb::cte:ss::root.with.ss, q56.sql::qb::cte:ss::root.with.ss, q60.sql::qb::cte:ss::root.with.ss, q64.sql::qb::cte:cross_sales::root.with.cross_sales
-- Columns: 30 columns
-- ============================================================
CREATE VIEW mv_018 AS
SELECT
    customer_address.ca_city,
    customer_address.ca_county,
    customer_address.ca_street_name,
    customer_address.ca_street_number,
    customer_address.ca_zip,
    date_dim.d_qoy,
    date_dim.d_year,
    item.i_item_id,
    item.i_item_sk,
    item.i_manufact_id,
    item.i_product_name,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3,
    SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price
FROM customer_address
INNER JOIN store_sales
    ON store_sales.ss_addr_sk = customer_address.ca_address_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
GROUP BY customer_address.ca_city, customer_address.ca_county, customer_address.ca_street_name, customer_address.ca_street_number, customer_address.ca_zip, date_dim.d_qoy, date_dim.d_year, item.i_item_id, item.i_item_sk, item.i_manufact_id, item.i_product_name;

-- ============================================================
-- MV: mv_019
-- Fact Table: store_sales
-- Tables: date_dim, item, promotion, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, promotion.p_promo_sk=store_sales.ss_promo_sk
-- QBs: q61.sql::qb::subquery:1::root.from.subquery1, q64.sql::qb::cte:cross_sales::root.with.cross_sales, q7.sql::qb::main:0::root, q80.sql::qb::cte:ssr::root.with.ssr
-- Columns: 31 columns
-- ============================================================
CREATE VIEW mv_019 AS
SELECT
    date_dim.d_year,
    item.i_item_id,
    item.i_item_sk,
    item.i_product_name,
    AVG(store_sales.ss_quantity) AS agg1,
    AVG(store_sales.ss_list_price) AS agg2,
    AVG(store_sales.ss_coupon_amt) AS agg3,
    AVG(store_sales.ss_sales_price) AS agg4,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3,
    SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price
FROM date_dim
INNER JOIN store_sales
    ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
INNER JOIN promotion
    ON promotion.p_promo_sk = store_sales.ss_promo_sk
GROUP BY date_dim.d_year, item.i_item_id, item.i_item_sk, item.i_product_name;

-- ============================================================
-- MV: mv_020
-- Fact Table: store_sales
-- Tables: store, store_returns, store_sales
-- Edges: store.s_store_sk=store_sales.ss_store_sk, store_returns.sr_item_sk=store_sales.ss_item_sk, store_returns.sr_ticket_number=store_sales.ss_ticket_number
-- QBs: q24a.sql::qb::cte:ssales::root.with.ssales, q24b.sql::qb::cte:ssales::root.with.ssales, q50.sql::qb::main:0::root, q64.sql::qb::cte:cross_sales::root.with.cross_sales
-- Columns: 29 columns
-- ============================================================
CREATE VIEW mv_020 AS
SELECT
    store.s_city,
    store.s_company_id,
    store.s_county,
    store.s_state,
    store.s_store_name,
    store.s_street_name,
    store.s_street_number,
    store.s_street_type,
    store.s_suite_number,
    store.s_zip,
    SUM(store_sales.ss_net_paid) AS netpaid,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3
FROM store
INNER JOIN store_sales
    ON store_sales.ss_store_sk = store.s_store_sk
INNER JOIN store_returns
    ON store_returns.sr_ticket_number = store_sales.ss_ticket_number
GROUP BY store.s_city, store.s_company_id, store.s_county, store.s_state, store.s_store_name, store.s_street_name, store.s_street_number, store.s_street_type, store.s_suite_number, store.s_zip;

-- ============================================================
-- MV: mv_021
-- Fact Table: store_sales
-- Tables: customer_demographics, date_dim, item, store_sales
-- Edges: customer_demographics.cd_demo_sk=store_sales.ss_cdemo_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk
-- QBs: q27.sql::qb::main:0::root, q64.sql::qb::cte:cross_sales::root.with.cross_sales, q7.sql::qb::main:0::root
-- Columns: 25 columns
-- ============================================================
CREATE VIEW mv_021 AS
SELECT
    date_dim.d_year,
    item.i_item_id,
    item.i_item_sk,
    item.i_product_name,
    AVG(store_sales.ss_quantity) AS agg1,
    AVG(store_sales.ss_list_price) AS agg2,
    AVG(store_sales.ss_coupon_amt) AS agg3,
    AVG(store_sales.ss_sales_price) AS agg4,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3
FROM customer_demographics
INNER JOIN store_sales
    ON store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
GROUP BY date_dim.d_year, item.i_item_id, item.i_item_sk, item.i_product_name;

-- ============================================================
-- MV: mv_022
-- Fact Table: store_sales
-- Tables: date_dim, item, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk
-- QBs: q10.sql::qb::subquery:1::root.exists.exists1, q11.sql::qb::union_branch:1::root.with.year_total.union.left, q14a.sql::qb::union_branch:1::root.with.cross_items.join.subquery1.intersect.left.intersect.left, q14a.sql::qb::union_branch:4::root.with.avg_sales.from.subquery2.union.left.union.left, q14a.sql::qb::union_branch:7::root.from.subquery3.union.left.union.left, ... (50 total)
-- Columns: 44 columns
-- ============================================================
CREATE VIEW mv_022 AS
SELECT
    date_dim.d_date,
    date_dim.d_moy,
    date_dim.d_qoy,
    date_dim.d_week_seq,
    date_dim.d_year,
    item.i_brand,
    item.i_brand_id,
    item.i_category,
    item.i_category_id,
    item.i_class,
    item.i_class_id,
    item.i_current_price,
    item.i_item_desc,
    item.i_item_id,
    item.i_item_sk,
    item.i_manager_id,
    item.i_manufact,
    item.i_manufact_id,
    item.i_product_name,
    store_sales.ss_customer_sk,
    store_sales.ss_item_sk,
    store_sales.ss_store_sk,
    COUNT(*) AS count_all,
    SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price,
    SUM(store_sales.ss_sales_price) AS sum_store_sales__ss_sales_price,
    AVG(store_sales.ss_quantity) AS agg1,
    AVG(store_sales.ss_list_price) AS agg2,
    AVG(store_sales.ss_coupon_amt) AS agg3,
    AVG(store_sales.ss_sales_price) AS agg4,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3,
    SUM(store_sales.ss_net_paid) AS year_total,
    SUM(store_sales.ss_net_profit) AS sum_store_sales__ss_net_profit
FROM date_dim
INNER JOIN store_sales
    ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
GROUP BY date_dim.d_date, date_dim.d_moy, date_dim.d_qoy, date_dim.d_week_seq, date_dim.d_year, item.i_brand, item.i_brand_id, item.i_category, item.i_category_id, item.i_class, item.i_class_id, item.i_current_price, item.i_item_desc, item.i_item_id, item.i_item_sk, item.i_manager_id, item.i_manufact, item.i_manufact_id, item.i_product_name, store_sales.ss_customer_sk, store_sales.ss_item_sk, store_sales.ss_store_sk;

-- ============================================================
-- MV: mv_023
-- Fact Table: store_sales
-- Tables: item, store, store_sales
-- Edges: item.i_item_sk=store_sales.ss_item_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q19.sql::qb::main:0::root, q24a.sql::qb::cte:ssales::root.with.ssales, q24b.sql::qb::cte:ssales::root.with.ssales, q27.sql::qb::main:0::root, q34.sql::qb::subquery:1::root.from.subquery1, ... (34 total)
-- Columns: 50 columns
-- ============================================================
CREATE VIEW mv_023 AS
SELECT
    item.i_brand,
    item.i_brand_id,
    item.i_category,
    item.i_class,
    item.i_color,
    item.i_current_price,
    item.i_item_sk,
    item.i_manager_id,
    item.i_manufact,
    item.i_manufact_id,
    item.i_product_name,
    item.i_size,
    item.i_units,
    store.s_city,
    store.s_company_id,
    store.s_company_name,
    store.s_county,
    store.s_state,
    store.s_store_id,
    store.s_store_name,
    store.s_store_sk,
    store.s_street_name,
    store.s_street_number,
    store.s_street_type,
    store.s_suite_number,
    store.s_zip,
    store_sales.ss_addr_sk,
    store_sales.ss_customer_sk,
    store_sales.ss_ticket_number,
    COUNT(*) AS count_all,
    SUM(store_sales.ss_sales_price) AS sum_store_sales__ss_sales_price,
    SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price,
    SUM(store_sales.ss_net_paid) AS netpaid,
    SUM(store_sales.ss_net_profit) AS sum_store_sales__ss_net_profit,
    SUM(store_sales.ss_coupon_amt) AS sum_store_sales__ss_coupon_amt,
    AVG(store_sales.ss_quantity) AS agg1,
    AVG(store_sales.ss_list_price) AS agg2,
    AVG(store_sales.ss_coupon_amt) AS agg3,
    AVG(store_sales.ss_sales_price) AS agg4,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_ext_list_price) AS list_price,
    SUM(store_sales.ss_ext_tax) AS extended_tax
FROM item
INNER JOIN store_sales
    ON store_sales.ss_item_sk = item.i_item_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
GROUP BY item.i_brand, item.i_brand_id, item.i_category, item.i_class, item.i_color, item.i_current_price, item.i_item_sk, item.i_manager_id, item.i_manufact, item.i_manufact_id, item.i_product_name, item.i_size, item.i_units, store.s_city, store.s_company_id, store.s_company_name, store.s_county, store.s_state, store.s_store_id, store.s_store_name, store.s_store_sk, store.s_street_name, store.s_street_number, store.s_street_type, store.s_suite_number, store.s_zip, store_sales.ss_addr_sk, store_sales.ss_customer_sk, store_sales.ss_ticket_number;

-- ============================================================
-- MV: mv_024
-- Fact Table: store_sales
-- Tables: date_dim, store, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q19.sql::qb::main:0::root, q27.sql::qb::main:0::root, q34.sql::qb::subquery:1::root.from.subquery1, q36.sql::qb::main:0::root, q43.sql::qb::main:0::root, ... (23 total)
-- Columns: 42 columns
-- ============================================================
CREATE VIEW mv_024 AS
SELECT
    date_dim.d_moy,
    date_dim.d_qoy,
    date_dim.d_year,
    store.s_city,
    store.s_company_id,
    store.s_company_name,
    store.s_county,
    store.s_state,
    store.s_store_id,
    store.s_store_name,
    store.s_store_sk,
    store.s_street_name,
    store.s_street_number,
    store.s_street_type,
    store.s_suite_number,
    store.s_zip,
    store_sales.ss_addr_sk,
    store_sales.ss_customer_sk,
    store_sales.ss_ticket_number,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_sales_price) AS sum_store_sales__ss_sales_price,
    SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price,
    SUM(store_sales.ss_net_profit) AS sum_store_sales__ss_net_profit,
    SUM(store_sales.ss_coupon_amt) AS sum_store_sales__ss_coupon_amt,
    AVG(store_sales.ss_quantity) AS agg1,
    AVG(store_sales.ss_list_price) AS agg2,
    AVG(store_sales.ss_coupon_amt) AS agg3,
    AVG(store_sales.ss_sales_price) AS agg4,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_ext_list_price) AS list_price,
    SUM(store_sales.ss_ext_tax) AS extended_tax
FROM date_dim
INNER JOIN store_sales
    ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
GROUP BY date_dim.d_moy, date_dim.d_qoy, date_dim.d_year, store.s_city, store.s_company_id, store.s_company_name, store.s_county, store.s_state, store.s_store_id, store.s_store_name, store.s_store_sk, store.s_street_name, store.s_street_number, store.s_street_type, store.s_suite_number, store.s_zip, store_sales.ss_addr_sk, store_sales.ss_customer_sk, store_sales.ss_ticket_number;

-- ============================================================
-- MV: mv_025
-- Fact Table: store_sales
-- Tables: customer, item, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, item.i_item_sk=store_sales.ss_item_sk
-- QBs: q11.sql::qb::union_branch:1::root.with.year_total.union.left, q19.sql::qb::main:0::root, q23a.sql::qb::cte:best_ss_customer::root.with.best_ss_customer, q23a.sql::qb::subquery:1::root.with.max_store_sales.from.subquery1, q23b.sql::qb::cte:best_ss_customer::root.with.best_ss_customer, ... (16 total)
-- Columns: 44 columns
-- ============================================================
CREATE VIEW mv_025 AS
SELECT
    customer.c_birth_country,
    customer.c_customer_id,
    customer.c_customer_sk,
    customer.c_email_address,
    customer.c_first_name,
    customer.c_last_name,
    customer.c_login,
    customer.c_preferred_cust_flag,
    item.i_brand,
    item.i_brand_id,
    item.i_color,
    item.i_current_price,
    item.i_item_sk,
    item.i_manager_id,
    item.i_manufact,
    item.i_manufact_id,
    item.i_product_name,
    item.i_size,
    item.i_units,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price,
    SUM(store_sales.ss_net_paid) AS sum_store_sales__ss_net_paid,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS s3
FROM customer
INNER JOIN store_sales
    ON store_sales.ss_customer_sk = customer.c_customer_sk
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
GROUP BY customer.c_birth_country, customer.c_customer_id, customer.c_customer_sk, customer.c_email_address, customer.c_first_name, customer.c_last_name, customer.c_login, customer.c_preferred_cust_flag, item.i_brand, item.i_brand_id, item.i_color, item.i_current_price, item.i_item_sk, item.i_manager_id, item.i_manufact, item.i_manufact_id, item.i_product_name, item.i_size, item.i_units;

-- ============================================================
-- MV: mv_026
-- Fact Table: store_sales
-- Tables: household_demographics, store, store_sales
-- Edges: household_demographics.hd_demo_sk=store_sales.ss_hdemo_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q34.sql::qb::subquery:1::root.from.subquery1, q46.sql::qb::subquery:1::root.from.subquery1, q64.sql::qb::cte:cross_sales::root.with.cross_sales, q68.sql::qb::subquery:1::root.from.subquery1, q73.sql::qb::subquery:1::root.from.subquery1, ... (15 total)
-- Columns: 28 columns
-- ============================================================
CREATE VIEW mv_026 AS
SELECT
    store.s_city,
    store.s_store_name,
    store.s_zip,
    store_sales.ss_addr_sk,
    store_sales.ss_customer_sk,
    store_sales.ss_ticket_number,
    COUNT(*) AS count_all,
    SUM(store_sales.ss_coupon_amt) AS sum_store_sales__ss_coupon_amt,
    SUM(store_sales.ss_net_profit) AS profit,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_ext_sales_price) AS extended_price,
    SUM(store_sales.ss_ext_list_price) AS list_price,
    SUM(store_sales.ss_ext_tax) AS extended_tax
FROM household_demographics
INNER JOIN store_sales
    ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
GROUP BY store.s_city, store.s_store_name, store.s_zip, store_sales.ss_addr_sk, store_sales.ss_customer_sk, store_sales.ss_ticket_number;

-- ============================================================
-- MV: mv_027
-- Fact Table: store_sales
-- Tables: customer_address, date_dim, store_sales
-- Edges: customer_address.ca_address_sk=store_sales.ss_addr_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk
-- QBs: q31.sql::qb::cte:ss::root.with.ss, q33.sql::qb::cte:ss::root.with.ss, q46.sql::qb::subquery:1::root.from.subquery1, q56.sql::qb::cte:ss::root.with.ss, q60.sql::qb::cte:ss::root.with.ss, ... (7 total)
-- Columns: 29 columns
-- ============================================================
CREATE VIEW mv_027 AS
SELECT
    customer_address.ca_city,
    customer_address.ca_county,
    customer_address.ca_street_name,
    customer_address.ca_street_number,
    customer_address.ca_zip,
    date_dim.d_qoy,
    date_dim.d_year,
    store_sales.ss_addr_sk,
    store_sales.ss_customer_sk,
    store_sales.ss_ticket_number,
    COUNT(*) AS cnt,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_coupon_amt) AS sum_store_sales__ss_coupon_amt,
    SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price,
    SUM(store_sales.ss_ext_list_price) AS list_price,
    SUM(store_sales.ss_ext_tax) AS extended_tax,
    SUM(store_sales.ss_net_profit) AS profit
FROM customer_address
INNER JOIN store_sales
    ON store_sales.ss_addr_sk = customer_address.ca_address_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
GROUP BY customer_address.ca_city, customer_address.ca_county, customer_address.ca_street_name, customer_address.ca_street_number, customer_address.ca_zip, date_dim.d_qoy, date_dim.d_year, store_sales.ss_addr_sk, store_sales.ss_customer_sk, store_sales.ss_ticket_number;

-- ============================================================
-- MV: mv_028
-- Fact Table: store_sales
-- Tables: date_dim, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk
-- QBs: q10.sql::qb::subquery:1::root.exists.exists1, q11.sql::qb::union_branch:1::root.with.year_total.union.left, q14a.sql::qb::union_branch:1::root.with.cross_items.join.subquery1.intersect.left.intersect.left, q14a.sql::qb::union_branch:4::root.with.avg_sales.from.subquery2.union.left.union.left, q14a.sql::qb::union_branch:7::root.from.subquery3.union.left.union.left, ... (64 total)
-- Columns: 32 columns
-- ============================================================
CREATE VIEW mv_028 AS
SELECT
    date_dim.d_date,
    date_dim.d_moy,
    date_dim.d_qoy,
    date_dim.d_week_seq,
    date_dim.d_year,
    store_sales.ss_addr_sk,
    store_sales.ss_customer_sk,
    store_sales.ss_item_sk,
    store_sales.ss_store_sk,
    store_sales.ss_ticket_number,
    COUNT(*) AS count_all,
    SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price,
    SUM(store_sales.ss_quantity) AS ss_qty,
    SUM(store_sales.ss_wholesale_cost) AS sum_store_sales__ss_wholesale_cost,
    SUM(store_sales.ss_sales_price) AS sum_store_sales__ss_sales_price,
    SUM(store_sales.ss_net_profit) AS sum_store_sales__ss_net_profit,
    SUM(store_sales.ss_coupon_amt) AS sum_store_sales__ss_coupon_amt,
    AVG(store_sales.ss_quantity) AS agg1,
    AVG(store_sales.ss_list_price) AS agg2,
    AVG(store_sales.ss_coupon_amt) AS agg3,
    AVG(store_sales.ss_sales_price) AS agg4,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_net_paid) AS year_total,
    SUM(store_sales.ss_ext_list_price) AS list_price,
    SUM(store_sales.ss_ext_tax) AS extended_tax
FROM date_dim
INNER JOIN store_sales
    ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
GROUP BY date_dim.d_date, date_dim.d_moy, date_dim.d_qoy, date_dim.d_week_seq, date_dim.d_year, store_sales.ss_addr_sk, store_sales.ss_customer_sk, store_sales.ss_item_sk, store_sales.ss_store_sk, store_sales.ss_ticket_number;

-- ============================================================
-- MV: mv_029
-- Fact Table: store_sales
-- Tables: item, store_sales
-- Edges: item.i_item_sk=store_sales.ss_item_sk
-- QBs: q02.sql::qb::main:0::root, q11.sql::qb::union_branch:1::root.with.year_total.union.left, q14a.sql::qb::union_branch:1::root.with.cross_items.join.subquery1.intersect.left.intersect.left, q14a.sql::qb::union_branch:7::root.from.subquery3.union.left.union.left, q14b.sql::qb::subquery:3::root.from.subquery3, ... (52 total)
-- Columns: 38 columns
-- ============================================================
CREATE VIEW mv_029 AS
SELECT
    item.i_brand,
    item.i_brand_id,
    item.i_category,
    item.i_category_id,
    item.i_class,
    item.i_class_id,
    item.i_color,
    item.i_current_price,
    item.i_item_desc,
    item.i_item_id,
    item.i_item_sk,
    item.i_manager_id,
    item.i_manufact,
    item.i_manufact_id,
    item.i_product_name,
    item.i_size,
    item.i_units,
    store_sales.ss_addr_sk,
    store_sales.ss_customer_sk,
    store_sales.ss_ticket_number,
    COUNT(*) AS count_all,
    SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price,
    SUM(store_sales.ss_net_paid) AS sum_store_sales__ss_net_paid,
    SUM(store_sales.ss_net_profit) AS sum_store_sales__ss_net_profit,
    SUM(store_sales.ss_coupon_amt) AS sum_store_sales__ss_coupon_amt,
    AVG(store_sales.ss_quantity) AS agg1,
    AVG(store_sales.ss_list_price) AS agg2,
    AVG(store_sales.ss_coupon_amt) AS agg3,
    AVG(store_sales.ss_sales_price) AS agg4,
    SUM(store_sales.ss_wholesale_cost) AS s1,
    SUM(store_sales.ss_list_price) AS s2,
    SUM(store_sales.ss_sales_price) AS sum_store_sales__ss_sales_price,
    SUM(store_sales.ss_ext_list_price) AS list_price,
    SUM(store_sales.ss_ext_tax) AS extended_tax
FROM item
INNER JOIN store_sales
    ON store_sales.ss_item_sk = item.i_item_sk
GROUP BY item.i_brand, item.i_brand_id, item.i_category, item.i_category_id, item.i_class, item.i_class_id, item.i_color, item.i_current_price, item.i_item_desc, item.i_item_id, item.i_item_sk, item.i_manager_id, item.i_manufact, item.i_manufact_id, item.i_product_name, item.i_size, item.i_units, store_sales.ss_addr_sk, store_sales.ss_customer_sk, store_sales.ss_ticket_number;

-- ============================================================
-- MV: mv_030
-- Fact Table: store_returns
-- Tables: date_dim, item, store_returns
-- Edges: date_dim.d_date_sk=store_returns.sr_returned_date_sk, item.i_item_sk=store_returns.sr_item_sk
-- QBs: q1.sql::qb::cte:customer_total_return::root.with.customer_total_return, q77.sql::qb::cte:sr::root.with.sr, q83.sql::qb::cte:sr_items::root.with.sr_items
-- Columns: 12 columns
-- ============================================================
CREATE VIEW mv_030 AS
SELECT
    item.i_item_id,
    store_returns.sr_customer_sk,
    store_returns.sr_store_sk,
    SUM(store_returns.sr_return_amt) AS sum_store_returns__sr_return_amt,
    SUM(store_returns.sr_net_loss) AS profit_loss,
    SUM(store_returns.sr_return_quantity) AS sr_item_qty
FROM date_dim
INNER JOIN store_returns
    ON store_returns.sr_returned_date_sk = date_dim.d_date_sk
INNER JOIN item
    ON item.i_item_sk = store_returns.sr_item_sk
GROUP BY item.i_item_id, store_returns.sr_customer_sk, store_returns.sr_store_sk;

-- ============================================================
-- MV: mv_031
-- Fact Table: customer
-- Tables: customer, customer_address, customer_demographics
-- Edges: customer.c_current_addr_sk=customer_address.ca_address_sk, customer.c_current_cdemo_sk=customer_demographics.cd_demo_sk
-- QBs: q10.sql::qb::main:0::root, q35.sql::qb::main:0::root, q69.sql::qb::main:0::root
-- Columns: 14 columns
-- ============================================================
CREATE VIEW mv_031 AS
SELECT
    customer_address.ca_state,
    customer_demographics.cd_credit_rating,
    customer_demographics.cd_dep_college_count,
    customer_demographics.cd_dep_count,
    customer_demographics.cd_dep_employed_count,
    customer_demographics.cd_education_status,
    customer_demographics.cd_gender,
    customer_demographics.cd_marital_status,
    customer_demographics.cd_purchase_estimate,
    COUNT(*) AS count_all,
    MIN(customer_demographics.cd_dep_count) AS min_customer_demographics__cd_dep_count,
    MAX(customer_demographics.cd_dep_count) AS max_customer_demographics__cd_dep_count,
    AVG(customer_demographics.cd_dep_count) AS avg_customer_demographics__cd_dep_count,
    MIN(customer_demographics.cd_dep_employed_count) AS min_customer_demographics__cd_dep_employed_count,
    MAX(customer_demographics.cd_dep_employed_count) AS max_customer_demographics__cd_dep_employed_count,
    AVG(customer_demographics.cd_dep_employed_count) AS avg_customer_demographics__cd_dep_employed_count,
    MIN(customer_demographics.cd_dep_college_count) AS min_customer_demographics__cd_dep_college_count,
    MAX(customer_demographics.cd_dep_college_count) AS max_customer_demographics__cd_dep_college_count,
    AVG(customer_demographics.cd_dep_college_count) AS avg_customer_demographics__cd_dep_college_count
FROM customer
INNER JOIN customer_address
    ON customer_address.ca_address_sk = customer.c_current_addr_sk
INNER JOIN customer_demographics
    ON customer_demographics.cd_demo_sk = customer.c_current_cdemo_sk
GROUP BY customer_address.ca_state, customer_demographics.cd_credit_rating, customer_demographics.cd_dep_college_count, customer_demographics.cd_dep_count, customer_demographics.cd_dep_employed_count, customer_demographics.cd_education_status, customer_demographics.cd_gender, customer_demographics.cd_marital_status, customer_demographics.cd_purchase_estimate;

-- ============================================================
-- MV: mv_032
-- Fact Table: customer
-- Tables: customer, customer_address
-- Edges: customer.c_current_addr_sk=customer_address.ca_address_sk
-- QBs: q10.sql::qb::main:0::root, q30.sql::qb::main:0::root, q35.sql::qb::main:0::root, q69.sql::qb::main:0::root, q8.sql::qb::subquery:3::root.join.subquery1.from.subquery2.intersect.right.from.subquery3, ... (6 total)
-- Columns: 27 columns
-- ============================================================
CREATE VIEW mv_032 AS
SELECT
    customer_address.ca_state,
    customer_address.ca_zip,
    COUNT(*) AS count_all
FROM customer
INNER JOIN customer_address
    ON customer_address.ca_address_sk = customer.c_current_addr_sk
GROUP BY customer_address.ca_state, customer_address.ca_zip;

-- ============================================================
-- MV: mv_033
-- Fact Table: customer
-- Tables: customer, customer_address
-- Edges: customer_address.ca_address_sk=customer.c_current_addr_sk
-- QBs: q46.sql::qb::main:0::root, q68.sql::qb::main:0::root
-- Columns: 6 columns
-- ============================================================
CREATE VIEW mv_033 AS
SELECT
    customer.c_current_addr_sk,
    customer.c_customer_sk,
    customer.c_first_name,
    customer.c_last_name,
    customer_address.ca_address_sk,
    customer_address.ca_city
FROM customer
INNER JOIN customer_address
    ON customer_address.ca_address_sk = customer.c_current_addr_sk;

-- ============================================================
-- MV: mv_034
-- Fact Table: web_sales
-- Tables: customer_address, date_dim, item, web_sales
-- Edges: customer_address.ca_address_sk=web_sales.ws_bill_addr_sk, date_dim.d_date_sk=web_sales.ws_sold_date_sk, item.i_item_sk=web_sales.ws_item_sk
-- QBs: q31.sql::qb::cte:ws::root.with.ws, q33.sql::qb::cte:ws::root.with.ws, q56.sql::qb::cte:ws::root.with.ws, q60.sql::qb::cte:ws::root.with.ws
-- Columns: 14 columns
-- ============================================================
CREATE VIEW mv_034 AS
SELECT
    customer_address.ca_county,
    date_dim.d_qoy,
    date_dim.d_year,
    item.i_item_id,
    item.i_manufact_id,
    SUM(web_sales.ws_ext_sales_price) AS sum_web_sales__ws_ext_sales_price
FROM customer_address
INNER JOIN web_sales
    ON web_sales.ws_bill_addr_sk = customer_address.ca_address_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = web_sales.ws_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = web_sales.ws_item_sk
GROUP BY customer_address.ca_county, date_dim.d_qoy, date_dim.d_year, item.i_item_id, item.i_manufact_id;

-- ============================================================
-- MV: mv_035
-- Fact Table: web_sales
-- Tables: customer_address, date_dim, web_sales, web_site
-- Edges: customer_address.ca_address_sk=web_sales.ws_ship_addr_sk, date_dim.d_date_sk=web_sales.ws_ship_date_sk, web_site.web_site_sk=web_sales.ws_web_site_sk
-- QBs: q94.sql::qb::main:0::root, q95.sql::qb::main:0::root
-- Columns: 12 columns
-- ============================================================
CREATE VIEW mv_035 AS
SELECT
    SUM(web_sales.ws_ext_ship_cost) AS `total shipping cost `,
    SUM(web_sales.ws_net_profit) AS `total net profit `
FROM customer_address
INNER JOIN web_sales
    ON web_sales.ws_ship_addr_sk = customer_address.ca_address_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = web_sales.ws_ship_date_sk
INNER JOIN web_site
    ON web_site.web_site_sk = web_sales.ws_web_site_sk;

-- ============================================================
-- MV: mv_036
-- Fact Table: web_sales
-- Tables: household_demographics, time_dim, web_page, web_sales
-- Edges: household_demographics.hd_demo_sk=web_sales.ws_ship_hdemo_sk, time_dim.t_time_sk=web_sales.ws_sold_time_sk, web_page.wp_web_page_sk=web_sales.ws_web_page_sk
-- QBs: q90.sql::qb::subquery:1::root.from.subquery1, q90.sql::qb::subquery:2::root.join.subquery2
-- Columns: 9 columns
-- ============================================================
CREATE VIEW mv_036 AS
SELECT
    COUNT(*) AS count_all
FROM household_demographics
INNER JOIN web_sales
    ON web_sales.ws_ship_hdemo_sk = household_demographics.hd_demo_sk
INNER JOIN time_dim
    ON time_dim.t_time_sk = web_sales.ws_sold_time_sk
INNER JOIN web_page
    ON web_page.wp_web_page_sk = web_sales.ws_web_page_sk;

-- ============================================================
-- MV: mv_037
-- Fact Table: web_sales
-- Tables: date_dim, item, web_sales
-- Edges: date_dim.d_date_sk=web_sales.ws_sold_date_sk, item.i_item_sk=web_sales.ws_item_sk
-- QBs: q10.sql::qb::subquery:2::root.exists.exists2, q11.sql::qb::union_branch:2::root.with.year_total.union.right, q12.sql::qb::main:0::root, q14a.sql::qb::union_branch:3::root.with.cross_items.join.subquery1.intersect.right, q14a.sql::qb::union_branch:6::root.with.avg_sales.from.subquery2.union.right, ... (27 total)
-- Columns: 30 columns
-- ============================================================
CREATE VIEW mv_037 AS
SELECT
    date_dim.d_date,
    date_dim.d_year,
    item.i_brand_id,
    item.i_category,
    item.i_category_id,
    item.i_class,
    item.i_class_id,
    item.i_current_price,
    item.i_item_desc,
    item.i_item_id,
    item.i_manufact_id,
    web_sales.ws_item_sk,
    COUNT(*) AS number_sales,
    SUM(web_sales.ws_net_paid) AS sum_web_sales__ws_net_paid,
    SUM(web_sales.ws_sales_price) AS sum_web_sales__ws_sales_price,
    SUM(web_sales.ws_ext_sales_price) AS sum_web_sales__ws_ext_sales_price,
    SUM(web_sales.ws_ext_discount_amt) AS `Excess Discount Amount `,
    AVG(web_sales.ws_ext_discount_amt) AS avg_web_sales__ws_ext_discount_amt
FROM date_dim
INNER JOIN web_sales
    ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
INNER JOIN item
    ON item.i_item_sk = web_sales.ws_item_sk
GROUP BY date_dim.d_date, date_dim.d_year, item.i_brand_id, item.i_category, item.i_category_id, item.i_class, item.i_class_id, item.i_current_price, item.i_item_desc, item.i_item_id, item.i_manufact_id, web_sales.ws_item_sk;

-- ============================================================
-- MV: mv_038
-- Fact Table: web_sales
-- Tables: customer, date_dim, web_sales
-- Edges: customer.c_customer_sk=web_sales.ws_bill_customer_sk, date_dim.d_date_sk=web_sales.ws_sold_date_sk
-- QBs: q11.sql::qb::union_branch:2::root.with.year_total.union.right, q23b.sql::qb::union_branch:2::root.from.subquery3.union.right, q38.sql::qb::union_branch:3::root.from.subquery1.intersect.right, q4.sql::qb::union_branch:3::root.with.year_total.union.right, q45.sql::qb::main:0::root, ... (7 total)
-- Columns: 26 columns
-- ============================================================
CREATE VIEW mv_038 AS
SELECT
    customer.c_birth_country,
    customer.c_customer_id,
    customer.c_email_address,
    customer.c_first_name,
    customer.c_last_name,
    customer.c_login,
    customer.c_preferred_cust_flag,
    date_dim.d_year,
    SUM(web_sales.ws_net_paid) AS year_total,
    SUM(web_sales.ws_sales_price) AS sum_web_sales__ws_sales_price
FROM customer
INNER JOIN web_sales
    ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = web_sales.ws_sold_date_sk
GROUP BY customer.c_birth_country, customer.c_customer_id, customer.c_email_address, customer.c_first_name, customer.c_last_name, customer.c_login, customer.c_preferred_cust_flag, date_dim.d_year;

-- ============================================================
-- MV: mv_039
-- Fact Table: web_sales
-- Tables: ship_mode, warehouse, web_sales
-- Edges: ship_mode.sm_ship_mode_sk=web_sales.ws_ship_mode_sk, warehouse.w_warehouse_sk=web_sales.ws_warehouse_sk
-- QBs: q62.sql::qb::main:0::root, q66.sql::qb::union_branch:1::root.from.subquery1.union.left
-- Columns: 19 columns
-- ============================================================
CREATE VIEW mv_039 AS
SELECT
    ship_mode.sm_type,
    warehouse.w_city,
    warehouse.w_country,
    warehouse.w_county,
    warehouse.w_state,
    warehouse.w_warehouse_name,
    warehouse.w_warehouse_sq_ft
FROM ship_mode
INNER JOIN web_sales
    ON web_sales.ws_ship_mode_sk = ship_mode.sm_ship_mode_sk
INNER JOIN warehouse
    ON warehouse.w_warehouse_sk = web_sales.ws_warehouse_sk
GROUP BY ship_mode.sm_type, warehouse.w_city, warehouse.w_country, warehouse.w_county, warehouse.w_state, warehouse.w_warehouse_name, warehouse.w_warehouse_sq_ft;

-- ============================================================
-- MV: mv_040
-- Fact Table: web_sales
-- Tables: date_dim, web_sales
-- Edges: date_dim.d_date_sk=web_sales.ws_sold_date_sk
-- QBs: q10.sql::qb::subquery:2::root.exists.exists2, q11.sql::qb::union_branch:2::root.with.year_total.union.right, q12.sql::qb::main:0::root, q14a.sql::qb::union_branch:3::root.with.cross_items.join.subquery1.intersect.right, q14a.sql::qb::union_branch:6::root.with.avg_sales.from.subquery2.union.right, ... (30 total)
-- Columns: 24 columns
-- ============================================================
CREATE VIEW mv_040 AS
SELECT
    date_dim.d_date,
    date_dim.d_qoy,
    date_dim.d_year,
    web_sales.ws_item_sk,
    COUNT(*) AS number_sales,
    SUM(web_sales.ws_net_paid) AS sum_web_sales__ws_net_paid,
    SUM(web_sales.ws_sales_price) AS sum_web_sales__ws_sales_price,
    SUM(web_sales.ws_ext_sales_price) AS sum_web_sales__ws_ext_sales_price,
    SUM(web_sales.ws_net_profit) AS profit,
    AVG(web_sales.ws_ext_discount_amt) AS avg_web_sales__ws_ext_discount_amt,
    SUM(web_sales.ws_ext_discount_amt) AS `Excess Discount Amount `
FROM date_dim
INNER JOIN web_sales
    ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
GROUP BY date_dim.d_date, date_dim.d_qoy, date_dim.d_year, web_sales.ws_item_sk;

-- ============================================================
-- MV: mv_041
-- Fact Table: web_sales
-- Tables: date_dim, web_sales
-- Edges: date_dim.d_date_sk=web_sales.ws_ship_date_sk
-- QBs: q62.sql::qb::main:0::root, q94.sql::qb::main:0::root, q95.sql::qb::main:0::root
-- Columns: 12 columns
-- ============================================================
CREATE VIEW mv_041 AS
SELECT
    SUM(web_sales.ws_ext_ship_cost) AS `total shipping cost `,
    SUM(web_sales.ws_net_profit) AS `total net profit `
FROM date_dim
INNER JOIN web_sales
    ON web_sales.ws_ship_date_sk = date_dim.d_date_sk;

-- ============================================================
-- MV: mv_042
-- Fact Table: web_sales
-- Tables: time_dim, web_sales
-- Edges: time_dim.t_time_sk=web_sales.ws_sold_time_sk
-- QBs: q66.sql::qb::union_branch:1::root.from.subquery1.union.left, q90.sql::qb::subquery:1::root.from.subquery1, q90.sql::qb::subquery:2::root.join.subquery2
-- Columns: 12 columns
-- ============================================================
CREATE VIEW mv_042 AS
SELECT
    COUNT(*) AS count_all
FROM time_dim
INNER JOIN web_sales
    ON web_sales.ws_sold_time_sk = time_dim.t_time_sk;

-- ============================================================
-- MV: mv_043
-- Fact Table: web_sales
-- Tables: web_page, web_sales
-- Edges: web_page.wp_web_page_sk=web_sales.ws_web_page_sk
-- QBs: q77.sql::qb::cte:ws::root.with.ws, q90.sql::qb::subquery:1::root.from.subquery1, q90.sql::qb::subquery:2::root.join.subquery2
-- Columns: 8 columns
-- ============================================================
CREATE VIEW mv_043 AS
SELECT
    web_page.wp_web_page_sk,
    SUM(web_sales.ws_ext_sales_price) AS sales,
    SUM(web_sales.ws_net_profit) AS profit,
    COUNT(*) AS count_all
FROM web_page
INNER JOIN web_sales
    ON web_sales.ws_web_page_sk = web_page.wp_web_page_sk
GROUP BY web_page.wp_web_page_sk;

-- ============================================================
-- MV: mv_044
-- Fact Table: catalog_sales
-- Tables: catalog_sales, date_dim, item, store, store_returns, store_sales
-- Edges: catalog_sales.cs_bill_customer_sk=store_returns.sr_customer_sk, catalog_sales.cs_item_sk=store_returns.sr_item_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk, ... (10 total)
-- QBs: q17.sql::qb::main:0::root, q25.sql::qb::main:0::root, q29.sql::qb::main:0::root
-- Columns: 29 columns
-- ============================================================
CREATE VIEW mv_044 AS
SELECT
    item.i_item_desc,
    item.i_item_id,
    store.s_state,
    store.s_store_id,
    store.s_store_name,
    SUM(store_sales.ss_quantity) AS store_sales_quantity,
    SUM(store_returns.sr_return_quantity) AS store_returns_quantity,
    SUM(catalog_sales.cs_quantity) AS catalog_sales_quantity,
    COUNT(store_sales.ss_quantity) AS store_sales_quantitycount,
    AVG(store_sales.ss_quantity) AS avg_store_sales__ss_quantity,
    COUNT(store_returns.sr_return_quantity) AS as_store_returns_quantitycount,
    AVG(store_returns.sr_return_quantity) AS avg_store_returns__sr_return_quantity,
    COUNT(catalog_sales.cs_quantity) AS catalog_sales_quantitycount,
    AVG(catalog_sales.cs_quantity) AS avg_catalog_sales__cs_quantity,
    SUM(store_sales.ss_net_profit) AS store_sales_profit,
    SUM(store_returns.sr_net_loss) AS store_returns_loss,
    SUM(catalog_sales.cs_net_profit) AS catalog_sales_profit
FROM catalog_sales
INNER JOIN date_dim
    ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
INNER JOIN store_returns
    ON store_returns.sr_item_sk = catalog_sales.cs_item_sk
INNER JOIN store_sales
    ON store_sales.ss_ticket_number = store_returns.sr_ticket_number
INNER JOIN item
    ON item.i_item_sk = store_sales.ss_item_sk
INNER JOIN store
    ON store.s_store_sk = store_sales.ss_store_sk
GROUP BY item.i_item_desc, item.i_item_id, store.s_state, store.s_store_id, store.s_store_name;

-- ============================================================
-- MV: mv_045
-- Fact Table: catalog_sales
-- Tables: catalog_returns, catalog_sales, date_dim, item
-- Edges: catalog_sales.cs_item_sk=catalog_returns.cr_item_sk, catalog_sales.cs_item_sk=item.i_item_sk, catalog_sales.cs_order_number=catalog_returns.cr_order_number, ... (4 total)
-- QBs: q40.sql::qb::main:0::root, q80.sql::qb::cte:csr::root.with.csr
-- Columns: 19 columns
-- ============================================================
CREATE VIEW mv_045 AS
SELECT
    item.i_item_id,
    SUM(catalog_sales.cs_ext_sales_price) AS sales
FROM catalog_sales
LEFT JOIN catalog_returns
    ON catalog_returns.cr_order_number = catalog_sales.cs_order_number
INNER JOIN date_dim
    ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = catalog_sales.cs_item_sk
GROUP BY item.i_item_id;

-- ============================================================
-- MV: mv_046
-- Fact Table: catalog_sales
-- Tables: catalog_sales, customer_address, date_dim, item
-- Edges: catalog_sales.cs_bill_addr_sk=customer_address.ca_address_sk, catalog_sales.cs_item_sk=item.i_item_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk
-- QBs: q33.sql::qb::cte:cs::root.with.cs, q56.sql::qb::cte:cs::root.with.cs, q60.sql::qb::cte:cs::root.with.cs
-- Columns: 12 columns
-- ============================================================
CREATE VIEW mv_046 AS
SELECT
    item.i_item_id,
    item.i_manufact_id,
    SUM(catalog_sales.cs_ext_sales_price) AS total_sales
FROM catalog_sales
INNER JOIN customer_address
    ON customer_address.ca_address_sk = catalog_sales.cs_bill_addr_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = catalog_sales.cs_item_sk
GROUP BY item.i_item_id, item.i_manufact_id;

-- ============================================================
-- MV: mv_047
-- Fact Table: catalog_sales
-- Tables: catalog_sales, customer_demographics, date_dim, item
-- Edges: catalog_sales.cs_bill_cdemo_sk=customer_demographics.cd_demo_sk, catalog_sales.cs_item_sk=item.i_item_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk
-- QBs: q18.sql::qb::main:0::root, q26.sql::qb::main:0::root
-- Columns: 19 columns
-- ============================================================
CREATE VIEW mv_047 AS
SELECT
    item.i_item_id,
    AVG(catalog_sales.cs_quantity) AS agg1,
    AVG(catalog_sales.cs_list_price) AS agg2,
    AVG(catalog_sales.cs_coupon_amt) AS agg3,
    AVG(catalog_sales.cs_sales_price) AS agg4
FROM catalog_sales
INNER JOIN customer_demographics
    ON customer_demographics.cd_demo_sk = catalog_sales.cs_bill_cdemo_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = catalog_sales.cs_item_sk
GROUP BY item.i_item_id;

-- ============================================================
-- MV: mv_048
-- Fact Table: catalog_sales
-- Tables: catalog_sales, customer, customer_address, date_dim
-- Edges: catalog_sales.cs_bill_customer_sk=customer.c_customer_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk, customer.c_current_addr_sk=customer_address.ca_address_sk
-- QBs: q15.sql::qb::main:0::root, q18.sql::qb::main:0::root
-- Columns: 22 columns
-- ============================================================
CREATE VIEW mv_048 AS
SELECT
    customer_address.ca_zip,
    SUM(catalog_sales.cs_sales_price) AS sum_catalog_sales__cs_sales_price
FROM catalog_sales
INNER JOIN customer
    ON customer.c_customer_sk = catalog_sales.cs_bill_customer_sk
INNER JOIN customer_address
    ON customer_address.ca_address_sk = customer.c_current_addr_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
GROUP BY customer_address.ca_zip;

-- ============================================================
-- MV: mv_049
-- Fact Table: catalog_sales
-- Tables: catalog_sales, date_dim, item, promotion
-- Edges: catalog_sales.cs_item_sk=item.i_item_sk, catalog_sales.cs_promo_sk=promotion.p_promo_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk
-- QBs: q26.sql::qb::main:0::root, q80.sql::qb::cte:csr::root.with.csr
-- Columns: 22 columns
-- ============================================================
CREATE VIEW mv_049 AS
SELECT
    item.i_item_id,
    SUM(catalog_sales.cs_ext_sales_price) AS sales,
    AVG(catalog_sales.cs_quantity) AS agg1,
    AVG(catalog_sales.cs_list_price) AS agg2,
    AVG(catalog_sales.cs_coupon_amt) AS agg3,
    AVG(catalog_sales.cs_sales_price) AS agg4
FROM catalog_sales
INNER JOIN date_dim
    ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = catalog_sales.cs_item_sk
INNER JOIN promotion
    ON promotion.p_promo_sk = catalog_sales.cs_promo_sk
GROUP BY item.i_item_id;

-- ============================================================
-- MV: mv_050
-- Fact Table: catalog_sales
-- Tables: catalog_sales, date_dim, item
-- Edges: catalog_sales.cs_item_sk=item.i_item_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk
-- QBs: q10.sql::qb::subquery:3::root.exists.exists3, q14a.sql::qb::union_branch:2::root.with.cross_items.join.subquery1.intersect.left.intersect.right, q14a.sql::qb::union_branch:5::root.with.avg_sales.from.subquery2.union.left.union.right, q14a.sql::qb::union_branch:8::root.from.subquery3.union.left.union.right, q14b.sql::qb::union_branch:2::root.with.cross_items.join.subquery1.intersect.left.intersect.right, ... (25 total)
-- Columns: 37 columns
-- ============================================================
CREATE VIEW mv_050 AS
SELECT
    catalog_sales.cs_bill_customer_sk,
    catalog_sales.cs_call_center_sk,
    catalog_sales.cs_item_sk,
    date_dim.d_moy,
    date_dim.d_year,
    item.i_brand,
    item.i_brand_id,
    item.i_category,
    item.i_category_id,
    item.i_class,
    item.i_class_id,
    item.i_current_price,
    item.i_item_desc,
    item.i_item_id,
    item.i_manufact_id,
    SUM(catalog_sales.cs_ext_sales_price) AS sum_catalog_sales__cs_ext_sales_price,
    SUM(catalog_sales.cs_ext_discount_amt) AS `excess discount amount`,
    SUM(catalog_sales.cs_sales_price) AS sum_catalog_sales__cs_sales_price,
    AVG(catalog_sales.cs_quantity) AS agg1,
    AVG(catalog_sales.cs_list_price) AS agg2,
    AVG(catalog_sales.cs_coupon_amt) AS agg3,
    AVG(catalog_sales.cs_sales_price) AS agg4,
    AVG(catalog_sales.cs_ext_discount_amt) AS avg_catalog_sales__cs_ext_discount_amt,
    SUM(catalog_sales.cs_net_profit) AS profit,
    COUNT(*) AS number_sales
FROM catalog_sales
INNER JOIN date_dim
    ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = catalog_sales.cs_item_sk
GROUP BY catalog_sales.cs_bill_customer_sk, catalog_sales.cs_call_center_sk, catalog_sales.cs_item_sk, date_dim.d_moy, date_dim.d_year, item.i_brand, item.i_brand_id, item.i_category, item.i_category_id, item.i_class, item.i_class_id, item.i_current_price, item.i_item_desc, item.i_item_id, item.i_manufact_id;

-- ============================================================
-- MV: mv_051
-- Fact Table: catalog_sales
-- Tables: catalog_sales, customer, date_dim
-- Edges: catalog_sales.cs_bill_customer_sk=customer.c_customer_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk
-- QBs: q15.sql::qb::main:0::root, q18.sql::qb::main:0::root, q23b.sql::qb::union_branch:1::root.from.subquery3.union.left, q38.sql::qb::union_branch:2::root.from.subquery1.intersect.left.intersect.right, q4.sql::qb::union_branch:2::root.with.year_total.union.left.union.right, ... (6 total)
-- Columns: 31 columns
-- ============================================================
CREATE VIEW mv_051 AS
SELECT
    customer.c_birth_country,
    customer.c_customer_id,
    customer.c_email_address,
    customer.c_first_name,
    customer.c_last_name,
    customer.c_login,
    customer.c_preferred_cust_flag,
    date_dim.d_year,
    SUM(catalog_sales.cs_sales_price) AS sum_catalog_sales__cs_sales_price
FROM catalog_sales
INNER JOIN customer
    ON customer.c_customer_sk = catalog_sales.cs_bill_customer_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
GROUP BY customer.c_birth_country, customer.c_customer_id, customer.c_email_address, customer.c_first_name, customer.c_last_name, customer.c_login, customer.c_preferred_cust_flag, date_dim.d_year;

-- ============================================================
-- MV: mv_052
-- Fact Table: catalog_sales
-- Tables: call_center, catalog_sales, date_dim
-- Edges: call_center.cc_call_center_sk=catalog_sales.cs_call_center_sk, catalog_sales.cs_ship_date_sk=date_dim.d_date_sk
-- QBs: q16.sql::qb::main:0::root, q99.sql::qb::main:0::root
-- Columns: 15 columns
-- ============================================================
CREATE VIEW mv_052 AS
SELECT
    call_center.cc_name,
    SUM(catalog_sales.cs_ext_ship_cost) AS `total shipping cost `,
    SUM(catalog_sales.cs_net_profit) AS `total net profit `
FROM call_center
INNER JOIN catalog_sales
    ON catalog_sales.cs_call_center_sk = call_center.cc_call_center_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = catalog_sales.cs_ship_date_sk
GROUP BY call_center.cc_name;

-- ============================================================
-- MV: mv_053
-- Fact Table: catalog_sales
-- Tables: catalog_sales, ship_mode, warehouse
-- Edges: catalog_sales.cs_ship_mode_sk=ship_mode.sm_ship_mode_sk, catalog_sales.cs_warehouse_sk=warehouse.w_warehouse_sk
-- QBs: q66.sql::qb::union_branch:2::root.from.subquery1.union.right, q99.sql::qb::main:0::root
-- Columns: 19 columns
-- ============================================================
CREATE VIEW mv_053 AS
SELECT
    ship_mode.sm_type,
    warehouse.w_city,
    warehouse.w_country,
    warehouse.w_county,
    warehouse.w_state,
    warehouse.w_warehouse_name,
    warehouse.w_warehouse_sq_ft
FROM catalog_sales
INNER JOIN ship_mode
    ON ship_mode.sm_ship_mode_sk = catalog_sales.cs_ship_mode_sk
INNER JOIN warehouse
    ON warehouse.w_warehouse_sk = catalog_sales.cs_warehouse_sk
GROUP BY ship_mode.sm_type, warehouse.w_city, warehouse.w_country, warehouse.w_county, warehouse.w_state, warehouse.w_warehouse_name, warehouse.w_warehouse_sq_ft;

-- ============================================================
-- MV: mv_054
-- Fact Table: catalog_sales
-- Tables: catalog_sales, date_dim, warehouse
-- Edges: catalog_sales.cs_sold_date_sk=date_dim.d_date_sk, catalog_sales.cs_warehouse_sk=warehouse.w_warehouse_sk
-- QBs: q40.sql::qb::main:0::root, q66.sql::qb::union_branch:2::root.from.subquery1.union.right
-- Columns: 20 columns
-- ============================================================
CREATE VIEW mv_054 AS
SELECT
    date_dim.d_year,
    warehouse.w_city,
    warehouse.w_country,
    warehouse.w_county,
    warehouse.w_state,
    warehouse.w_warehouse_name,
    warehouse.w_warehouse_sq_ft
FROM catalog_sales
INNER JOIN date_dim
    ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
INNER JOIN warehouse
    ON warehouse.w_warehouse_sk = catalog_sales.cs_warehouse_sk
GROUP BY date_dim.d_year, warehouse.w_city, warehouse.w_country, warehouse.w_county, warehouse.w_state, warehouse.w_warehouse_name, warehouse.w_warehouse_sq_ft;

-- ============================================================
-- MV: mv_055
-- Fact Table: catalog_sales
-- Tables: catalog_sales, date_dim
-- Edges: catalog_sales.cs_sold_date_sk=date_dim.d_date_sk
-- QBs: q10.sql::qb::subquery:3::root.exists.exists3, q14a.sql::qb::union_branch:2::root.with.cross_items.join.subquery1.intersect.left.intersect.right, q14a.sql::qb::union_branch:5::root.with.avg_sales.from.subquery2.union.left.union.right, q14a.sql::qb::union_branch:8::root.from.subquery3.union.left.union.right, q14b.sql::qb::union_branch:2::root.with.cross_items.join.subquery1.intersect.left.intersect.right, ... (34 total)
-- Columns: 31 columns
-- ============================================================
CREATE VIEW mv_055 AS
SELECT
    catalog_sales.cs_bill_customer_sk,
    catalog_sales.cs_call_center_sk,
    catalog_sales.cs_item_sk,
    date_dim.d_moy,
    date_dim.d_year,
    SUM(catalog_sales.cs_ext_sales_price) AS sum_catalog_sales__cs_ext_sales_price,
    SUM(catalog_sales.cs_net_profit) AS sum_catalog_sales__cs_net_profit,
    SUM(catalog_sales.cs_quantity) AS catalog_sales_quantity,
    SUM(catalog_sales.cs_ext_discount_amt) AS `excess discount amount`,
    COUNT(catalog_sales.cs_quantity) AS catalog_sales_quantitycount,
    AVG(catalog_sales.cs_quantity) AS avg_catalog_sales__cs_quantity,
    SUM(catalog_sales.cs_sales_price) AS sum_catalog_sales__cs_sales_price,
    AVG(catalog_sales.cs_list_price) AS agg2,
    AVG(catalog_sales.cs_coupon_amt) AS agg3,
    AVG(catalog_sales.cs_sales_price) AS agg4,
    AVG(catalog_sales.cs_ext_discount_amt) AS avg_catalog_sales__cs_ext_discount_amt,
    COUNT(*) AS number_sales
FROM catalog_sales
INNER JOIN date_dim
    ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
GROUP BY catalog_sales.cs_bill_customer_sk, catalog_sales.cs_call_center_sk, catalog_sales.cs_item_sk, date_dim.d_moy, date_dim.d_year;

-- ============================================================
-- MV: mv_056
-- Fact Table: catalog_sales
-- Tables: call_center, catalog_sales
-- Edges: call_center.cc_call_center_sk=catalog_sales.cs_call_center_sk
-- QBs: q16.sql::qb::main:0::root, q57.sql::qb::cte:v1::root.with.v1, q99.sql::qb::main:0::root
-- Columns: 14 columns
-- ============================================================
CREATE VIEW mv_056 AS
SELECT
    call_center.cc_name,
    SUM(catalog_sales.cs_ext_ship_cost) AS `total shipping cost `,
    SUM(catalog_sales.cs_net_profit) AS `total net profit `,
    SUM(catalog_sales.cs_sales_price) AS sum_catalog_sales__cs_sales_price
FROM call_center
INNER JOIN catalog_sales
    ON catalog_sales.cs_call_center_sk = call_center.cc_call_center_sk
GROUP BY call_center.cc_name;

-- ============================================================
-- MV: mv_057
-- Fact Table: catalog_sales
-- Tables: catalog_sales, warehouse
-- Edges: catalog_sales.cs_warehouse_sk=warehouse.w_warehouse_sk
-- QBs: q40.sql::qb::main:0::root, q66.sql::qb::union_branch:2::root.from.subquery1.union.right, q99.sql::qb::main:0::root
-- Columns: 18 columns
-- ============================================================
CREATE VIEW mv_057 AS
SELECT
    warehouse.w_city,
    warehouse.w_country,
    warehouse.w_county,
    warehouse.w_state,
    warehouse.w_warehouse_name,
    warehouse.w_warehouse_sq_ft
FROM catalog_sales
INNER JOIN warehouse
    ON warehouse.w_warehouse_sk = catalog_sales.cs_warehouse_sk
GROUP BY warehouse.w_city, warehouse.w_country, warehouse.w_county, warehouse.w_state, warehouse.w_warehouse_name, warehouse.w_warehouse_sq_ft;

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
    date_dim.d_moy,
    item.i_current_price,
    item.i_item_desc,
    item.i_item_id,
    item.i_item_sk,
    warehouse.w_warehouse_name,
    warehouse.w_warehouse_sk,
    AVG(inventory.inv_quantity_on_hand) AS avg_inventory__inv_quantity_on_hand
FROM catalog_sales
INNER JOIN item
    ON item.i_item_sk = catalog_sales.cs_item_sk
INNER JOIN inventory
    ON inventory.inv_item_sk = item.i_item_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = inventory.inv_date_sk
INNER JOIN warehouse
    ON warehouse.w_warehouse_sk = inventory.inv_warehouse_sk
GROUP BY date_dim.d_moy, item.i_current_price, item.i_item_desc, item.i_item_id, item.i_item_sk, warehouse.w_warehouse_name, warehouse.w_warehouse_sk;

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
    date_dim.d_moy,
    item.i_current_price,
    item.i_item_desc,
    item.i_item_id,
    item.i_item_sk,
    AVG(inventory.inv_quantity_on_hand) AS avg_inventory__inv_quantity_on_hand
FROM catalog_sales
INNER JOIN item
    ON item.i_item_sk = catalog_sales.cs_item_sk
INNER JOIN inventory
    ON inventory.inv_item_sk = item.i_item_sk
INNER JOIN date_dim
    ON date_dim.d_date_sk = inventory.inv_date_sk
INNER JOIN store_sales
    ON store_sales.ss_item_sk = item.i_item_sk
GROUP BY date_dim.d_moy, item.i_current_price, item.i_item_desc, item.i_item_id, item.i_item_sk;

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
    date_dim.d_moy,
    item.i_current_price,
    item.i_item_desc,
    item.i_item_id,
    item.i_item_sk,
    warehouse.w_warehouse_name,
    warehouse.w_warehouse_sk,
    AVG(inventory.inv_quantity_on_hand) AS avg_inventory__inv_quantity_on_hand
FROM date_dim
INNER JOIN inventory
    ON inventory.inv_date_sk = date_dim.d_date_sk
INNER JOIN item
    ON item.i_item_sk = inventory.inv_item_sk
INNER JOIN store_sales
    ON store_sales.ss_item_sk = item.i_item_sk
INNER JOIN warehouse
    ON warehouse.w_warehouse_sk = inventory.inv_warehouse_sk
GROUP BY date_dim.d_moy, item.i_current_price, item.i_item_desc, item.i_item_id, item.i_item_sk, warehouse.w_warehouse_name, warehouse.w_warehouse_sk;

-- ============================================================
-- MV: mv_061
-- Fact Table: web_returns
-- Tables: date_dim, item, web_returns, web_sales
-- Edges: date_dim.d_date_sk=web_sales.ws_sold_date_sk, item.i_item_sk=web_sales.ws_item_sk, web_sales.ws_item_sk=web_returns.wr_item_sk, ... (4 total)
-- QBs: q49.sql::qb::subquery:2::root.union.left.union.left.from.subquery1.from.subquery2, q75.sql::qb::union_branch:3::root.with.all_sales.from.subquery1.union.right, q78.sql::qb::cte:ws::root.with.ws, q80.sql::qb::cte:wsr::root.with.wsr
-- Columns: 28 columns
-- ============================================================
CREATE VIEW mv_061 AS
SELECT
    date_dim.d_year,
    web_sales.ws_bill_customer_sk,
    web_sales.ws_item_sk,
    SUM(web_sales.ws_ext_sales_price) AS sales,
    SUM(web_sales.ws_quantity) AS ws_qty,
    SUM(web_sales.ws_wholesale_cost) AS ws_wc,
    SUM(web_sales.ws_sales_price) AS ws_sp
FROM date_dim
INNER JOIN web_sales
    ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
LEFT JOIN web_returns
    ON web_returns.wr_order_number = web_sales.ws_order_number
GROUP BY date_dim.d_year, web_sales.ws_bill_customer_sk, web_sales.ws_item_sk;

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
    item.i_item_id,
    web_returns.wr_returning_customer_sk,
    SUM(web_returns.wr_return_amt) AS sum_web_returns__wr_return_amt,
    SUM(web_returns.wr_net_loss) AS profit_loss,
    SUM(web_returns.wr_return_quantity) AS wr_item_qty
FROM date_dim
INNER JOIN web_returns
    ON web_returns.wr_returned_date_sk = date_dim.d_date_sk
INNER JOIN item
    ON item.i_item_sk = web_returns.wr_item_sk
GROUP BY item.i_item_id, web_returns.wr_returning_customer_sk;

-- ============================================================
-- MV: mv_063
-- Fact Table: web_returns
-- Tables: date_dim, web_sales
-- Edges: date_dim.d_date_sk=web_sales.ws_sold_date_sk
-- QBs: q49.sql::qb::subquery:2::root.union.left.union.left.from.subquery1.from.subquery2, q75.sql::qb::union_branch:3::root.with.all_sales.from.subquery1.union.right, q78.sql::qb::cte:ws::root.with.ws, q80.sql::qb::cte:wsr::root.with.wsr, q85.sql::qb::main:0::root
-- Columns: 17 columns
-- ============================================================
CREATE VIEW mv_063 AS
SELECT
    date_dim.d_year,
    web_sales.ws_bill_customer_sk,
    web_sales.ws_item_sk,
    AVG(web_sales.ws_quantity) AS avg_web_sales__ws_quantity,
    SUM(web_sales.ws_ext_sales_price) AS sales,
    SUM(web_sales.ws_quantity) AS ws_qty,
    SUM(web_sales.ws_wholesale_cost) AS ws_wc,
    SUM(web_sales.ws_sales_price) AS ws_sp
FROM date_dim
INNER JOIN web_sales
    ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
GROUP BY date_dim.d_year, web_sales.ws_bill_customer_sk, web_sales.ws_item_sk;

-- ============================================================
-- MV: mv_064
-- Fact Table: catalog_returns
-- Tables: catalog_returns, catalog_sales, date_dim, item
-- Edges: catalog_sales.cs_item_sk=catalog_returns.cr_item_sk, catalog_sales.cs_item_sk=item.i_item_sk, catalog_sales.cs_order_number=catalog_returns.cr_order_number, ... (4 total)
-- QBs: q49.sql::qb::subquery:4::root.union.left.union.right.from.subquery3.from.subquery4, q75.sql::qb::union_branch:1::root.with.all_sales.from.subquery1.union.left.union.left, q78.sql::qb::cte:cs::root.with.cs
-- Columns: 23 columns
-- ============================================================
CREATE VIEW mv_064 AS
SELECT
    catalog_sales.cs_bill_customer_sk,
    catalog_sales.cs_item_sk,
    date_dim.d_year,
    SUM(catalog_sales.cs_quantity) AS cs_qty,
    SUM(catalog_sales.cs_wholesale_cost) AS cs_wc,
    SUM(catalog_sales.cs_sales_price) AS cs_sp
FROM catalog_sales
LEFT JOIN catalog_returns
    ON catalog_returns.cr_order_number = catalog_sales.cs_order_number
INNER JOIN date_dim
    ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
INNER JOIN item
    ON item.i_item_sk = catalog_sales.cs_item_sk
GROUP BY catalog_sales.cs_bill_customer_sk, catalog_sales.cs_item_sk, date_dim.d_year;

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
    catalog_returns.cr_returning_customer_sk,
    item.i_item_id,
    SUM(catalog_returns.cr_return_quantity) AS cr_item_qty,
    SUM(catalog_returns.cr_return_amount) AS returns,
    SUM(catalog_returns.cr_net_loss) AS profit_loss,
    SUM(catalog_returns.cr_return_amt_inc_tax) AS ctr_total_return
FROM catalog_returns
INNER JOIN date_dim
    ON date_dim.d_date_sk = catalog_returns.cr_returned_date_sk
INNER JOIN item
    ON item.i_item_sk = catalog_returns.cr_item_sk
GROUP BY catalog_returns.cr_returning_customer_sk, item.i_item_id;

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
    catalog_returns.cr_returning_customer_sk,
    SUM(catalog_returns.cr_return_quantity) AS cr_item_qty,
    SUM(catalog_returns.cr_return_amount) AS returns,
    SUM(catalog_returns.cr_net_loss) AS sum_catalog_returns__cr_net_loss,
    SUM(catalog_returns.cr_return_amt_inc_tax) AS ctr_total_return
FROM catalog_returns
INNER JOIN date_dim
    ON date_dim.d_date_sk = catalog_returns.cr_returned_date_sk
GROUP BY catalog_returns.cr_returning_customer_sk;

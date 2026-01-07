-- ============================================================
-- MV: mv_021
-- Fact Table: store_sales
-- Tables: item, store, store_sales
-- Edges: item.i_item_sk=store_sales.ss_item_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q19.sql::qb::main:0::root, q24a.sql::qb::cte:ssales::root.with.ssales, q24b.sql::qb::cte:ssales::root.with.ssales, q27.sql::qb::main:0::root, q34.sql::qb::subquery:1::root.from.subquery1, ... (34 total)
-- Columns: 50 columns
-- ============================================================
CREATE VIEW mv_021 AS
SELECT
  i.i_brand,
  i.i_brand_id,
  i.i_category,
  i.i_class,
  i.i_color,
  i.i_current_price,
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
  SUM(ss.ss_net_profit) AS sum_ss__ss_net_profit,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  SUM(ss.ss_sales_price) AS sum_ss__ss_sales_price,
  SUM(ss.ss_net_paid) AS netpaid,
  COUNT(*) AS count_all,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax,
  SUM(ss.ss_coupon_amt) AS sum_ss__ss_coupon_amt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  AVG(ss.ss_quantity) AS agg1,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4
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
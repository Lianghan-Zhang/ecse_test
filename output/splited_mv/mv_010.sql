-- ============================================================
-- MV: mv_010
-- Fact Table: store_sales
-- Tables: date_dim, item, store, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q19.sql::qb::main:0::root, q27.sql::qb::main:0::root, q34.sql::qb::subquery:1::root.from.subquery1, q43.sql::qb::main:0::root, q46.sql::qb::subquery:1::root.from.subquery1, ... (19 total)
-- Columns: 47 columns
-- ============================================================
CREATE VIEW mv_010 AS
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
  SUM(ss.ss_sales_price) AS sum_ss__ss_sales_price,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  SUM(ss.ss_net_profit) AS sum_ss__ss_net_profit,
  COUNT(*) AS cnt,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax,
  SUM(ss.ss_coupon_amt) AS amt,
  AVG(ss.ss_quantity) AS agg1,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4
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
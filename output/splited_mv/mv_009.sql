-- ============================================================
-- MV: mv_009
-- Fact Table: store_sales
-- Tables: date_dim, inventory, item, store_sales
-- Edges: date_dim.d_date_sk=inventory.inv_date_sk, inventory.inv_item_sk=item.i_item_sk, item.i_item_sk=store_sales.ss_item_sk
-- QBs: q10.sql::qb::subquery:1::root.exists.exists1, q11.sql::qb::union_branch:1::root.with.year_total.union.left, q14a.sql::qb::union_branch:4::root.with.avg_sales.from.subquery2.union.left.union.left, q14a.sql::qb::union_branch:7::root.from.subquery3.union.left.union.left, q14b.sql::qb::subquery:3::root.from.subquery3, ... (63 total)
-- Columns: 63 columns
-- ============================================================
CREATE VIEW mv_009 AS
SELECT
  d1.d_year AS d1__d_year,
  d2.d_year AS d2__d_year,
  d3.d_year AS d3__d_year,
  d.d_date,
  d.d_moy,
  d.d_qoy,
  d.d_week_seq,
  d.d_year AS d__d_year,
  dt.d_year AS dt__d_year,
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
  SUM(ss.ss_net_profit) AS sum_ss__ss_net_profit,
  SUM(ss.ss_sales_price) AS sum_ss__ss_sales_price,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  AVG(ss.ss_quantity) AS agg1,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4,
  COUNT(*) AS count_all,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax,
  SUM(ss.ss_coupon_amt) AS sum_ss__ss_coupon_amt,
  SUM(ss.ss_wholesale_cost) AS sum_ss__ss_wholesale_cost,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_net_paid) AS sum_ss__ss_net_paid,
  SUM(ss.ss_quantity) AS ss_qty
FROM date_dim AS d
INNER JOIN inventory AS inv
  ON inv.inv_date_sk = d.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = inv.inv_item_sk
INNER JOIN store_sales AS ss
  ON ss.ss_item_sk = i.i_item_sk
GROUP BY
  d1.d_year,
  d2.d_year,
  d3.d_year,
  d.d_date,
  d.d_moy,
  d.d_qoy,
  d.d_week_seq,
  d.d_year,
  dt.d_year,
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
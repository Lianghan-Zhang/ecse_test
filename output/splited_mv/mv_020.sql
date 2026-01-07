-- ============================================================
-- MV: mv_020
-- Fact Table: store_sales
-- Tables: date_dim, item, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk
-- QBs: q10.sql::qb::subquery:1::root.exists.exists1, q11.sql::qb::union_branch:1::root.with.year_total.union.left, q14a.sql::qb::union_branch:4::root.with.avg_sales.from.subquery2.union.left.union.left, q14a.sql::qb::union_branch:7::root.from.subquery3.union.left.union.left, q14b.sql::qb::subquery:3::root.from.subquery3, ... (42 total)
-- Columns: 41 columns
-- ============================================================
CREATE VIEW mv_020 AS
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
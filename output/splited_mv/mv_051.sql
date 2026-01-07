-- ============================================================
-- MV: mv_051
-- Fact Table: catalog_sales
-- Tables: catalog_sales, date_dim, item
-- Edges: catalog_sales.cs_item_sk=item.i_item_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk
-- QBs: q10.sql::qb::subquery:3::root.exists.exists3, q14a.sql::qb::union_branch:5::root.with.avg_sales.from.subquery2.union.left.union.right, q14a.sql::qb::union_branch:8::root.from.subquery3.union.left.union.right, q14b.sql::qb::union_branch:5::root.with.avg_sales.from.subquery2.union.left.union.right, q18.sql::qb::main:0::root, ... (24 total)
-- Columns: 37 columns
-- ============================================================
CREATE VIEW mv_051 AS
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
  SUM(cs.cs_ext_sales_price) AS sum_cs__cs_ext_sales_price,
  SUM(cs.cs_sales_price) AS sum_cs__cs_sales_price,
  COUNT(*) AS number_sales,
  SUM(cs.cs_net_profit) AS profit,
  SUM(cs.cs_ext_discount_amt) AS `excess discount amount`,
  AVG(cs.cs_ext_discount_amt) AS avg_cs__cs_ext_discount_amt,
  AVG(cs.cs_quantity) AS agg1,
  AVG(cs.cs_list_price) AS agg2,
  AVG(cs.cs_coupon_amt) AS agg3,
  AVG(cs.cs_sales_price) AS agg4
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
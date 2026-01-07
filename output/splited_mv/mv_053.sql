-- ============================================================
-- MV: mv_053
-- Fact Table: catalog_sales
-- Tables: catalog_sales, date_dim, item
-- Edges: catalog_sales.cs_item_sk=ics.i_item_sk, catalog_sales.cs_sold_date_sk=d2.d_date_sk
-- QBs: q14a.sql::qb::union_branch:2::root.with.cross_items.join.subquery1.intersect.left.intersect.right, q14b.sql::qb::union_branch:2::root.with.cross_items.join.subquery1.intersect.left.intersect.right
-- Columns: 8 columns
-- ============================================================
CREATE VIEW mv_053 AS
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
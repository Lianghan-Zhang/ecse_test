-- ============================================================
-- MV: mv_040
-- Fact Table: web_sales
-- Tables: date_dim, item, web_sales
-- Edges: d3.d_date_sk=web_sales.ws_sold_date_sk, iws.i_item_sk=web_sales.ws_item_sk
-- QBs: q14a.sql::qb::union_branch:3::root.with.cross_items.join.subquery1.intersect.right, q14b.sql::qb::union_branch:3::root.with.cross_items.join.subquery1.intersect.right
-- Columns: 8 columns
-- ============================================================
CREATE VIEW mv_040 AS
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
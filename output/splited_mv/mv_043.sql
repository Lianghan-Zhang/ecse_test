-- ============================================================
-- MV: mv_043
-- Fact Table: web_sales
-- Tables: item, web_sales
-- Edges: item.i_item_sk=web_sales.ws_item_sk
-- QBs: q10.sql::qb::subquery:2::root.exists.exists2, q11.sql::qb::union_branch:2::root.with.year_total.union.right, q12.sql::qb::main:0::root, q14a.sql::qb::union_branch:6::root.with.avg_sales.from.subquery2.union.right, q14a.sql::qb::union_branch:9::root.from.subquery3.union.right, ... (26 total)
-- Columns: 24 columns
-- ============================================================
CREATE VIEW mv_043 AS
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
  SUM(ws.ws_sales_price) AS sum_ws__ws_sales_price,
  AVG(ws.ws_ext_discount_amt) AS avg_ws__ws_ext_discount_amt,
  SUM(ws.ws_ext_discount_amt) AS `Excess Discount Amount `,
  SUM(ws.ws_ext_sales_price) AS sum_ws__ws_ext_sales_price,
  COUNT(*) AS number_sales
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
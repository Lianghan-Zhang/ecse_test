-- ============================================================
-- MV: mv_038
-- Fact Table: web_sales
-- Tables: date_dim, item, web_sales
-- Edges: date_dim.d_date_sk=web_sales.ws_sold_date_sk, item.i_item_sk=web_sales.ws_item_sk
-- QBs: q10.sql::qb::subquery:2::root.exists.exists2, q11.sql::qb::union_branch:2::root.with.year_total.union.right, q12.sql::qb::main:0::root, q14a.sql::qb::union_branch:6::root.with.avg_sales.from.subquery2.union.right, q14a.sql::qb::union_branch:9::root.from.subquery3.union.right, ... (24 total)
-- Columns: 30 columns
-- ============================================================
CREATE VIEW mv_038 AS
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
  SUM(ws.ws_sales_price) AS sum_ws__ws_sales_price,
  AVG(ws.ws_ext_discount_amt) AS avg_ws__ws_ext_discount_amt,
  SUM(ws.ws_ext_discount_amt) AS `Excess Discount Amount `,
  SUM(ws.ws_ext_sales_price) AS sum_ws__ws_ext_sales_price,
  SUM(ws.ws_net_paid) AS year_total,
  COUNT(*) AS number_sales
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
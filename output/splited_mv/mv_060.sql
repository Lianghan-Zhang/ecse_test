-- ============================================================
-- MV: mv_060
-- Fact Table: web_returns
-- Tables: date_dim, item, web_returns, web_sales
-- Edges: date_dim.d_date_sk=web_sales.ws_sold_date_sk, item.i_item_sk=web_sales.ws_item_sk, web_sales.ws_item_sk=web_returns.wr_item_sk, ... (4 total)
-- QBs: q75.sql::qb::union_branch:3::root.with.all_sales.from.subquery1.union.right, q78.sql::qb::cte:ws::root.with.ws, q80.sql::qb::cte:wsr::root.with.wsr
-- Columns: 26 columns
-- ============================================================
CREATE VIEW mv_060 AS
SELECT
  d.d_year,
  ws.ws_bill_customer_sk,
  ws.ws_item_sk,
  SUM(ws.ws_quantity) AS ws_qty,
  SUM(ws.ws_wholesale_cost) AS ws_wc,
  SUM(ws.ws_sales_price) AS ws_sp,
  SUM(ws.ws_ext_sales_price) AS sales
FROM date_dim AS d
INNER JOIN web_sales AS ws
  ON ws.ws_item_sk = i.i_item_sk
LEFT JOIN web_returns AS wr
  ON wr.wr_item_sk = ws.ws_item_sk
GROUP BY
  d.d_year,
  ws.ws_bill_customer_sk,
  ws.ws_item_sk;
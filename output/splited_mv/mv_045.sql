-- ============================================================
-- MV: mv_045
-- Fact Table: web_sales
-- Tables: web_page, web_sales
-- Edges: web_page.wp_web_page_sk=web_sales.ws_web_page_sk
-- QBs: q77.sql::qb::cte:ws::root.with.ws, q90.sql::qb::subquery:1::root.from.subquery1, q90.sql::qb::subquery:2::root.join.subquery2
-- Columns: 8 columns
-- ============================================================
CREATE VIEW mv_045 AS
SELECT
  wp.wp_web_page_sk,
  COUNT(*) AS count_all,
  SUM(ws.ws_ext_sales_price) AS sales,
  SUM(ws.ws_net_profit) AS profit
FROM web_page AS wp
INNER JOIN web_sales AS ws
  ON ws.ws_web_page_sk = wp.wp_web_page_sk
GROUP BY
  wp.wp_web_page_sk;
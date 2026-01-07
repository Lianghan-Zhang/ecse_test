-- ============================================================
-- MV: mv_037
-- Fact Table: web_sales
-- Tables: household_demographics, time_dim, web_page, web_sales
-- Edges: household_demographics.hd_demo_sk=web_sales.ws_ship_hdemo_sk, time_dim.t_time_sk=web_sales.ws_sold_time_sk, web_page.wp_web_page_sk=web_sales.ws_web_page_sk
-- QBs: q90.sql::qb::subquery:1::root.from.subquery1, q90.sql::qb::subquery:2::root.join.subquery2
-- Columns: 9 columns
-- ============================================================
CREATE VIEW mv_037 AS
SELECT
  COUNT(*) AS count_all
FROM household_demographics AS hd
INNER JOIN web_sales AS ws
  ON ws.ws_ship_hdemo_sk = hd.hd_demo_sk
INNER JOIN time_dim AS t
  ON t.t_time_sk = ws.ws_sold_time_sk
INNER JOIN web_page AS wp
  ON wp.wp_web_page_sk = ws.ws_web_page_sk;
-- ============================================================
-- MV: mv_044
-- Fact Table: web_sales
-- Tables: time_dim, web_sales
-- Edges: time_dim.t_time_sk=web_sales.ws_sold_time_sk
-- QBs: q66.sql::qb::union_branch:1::root.from.subquery1.union.left, q90.sql::qb::subquery:1::root.from.subquery1, q90.sql::qb::subquery:2::root.join.subquery2
-- Columns: 12 columns
-- ============================================================
CREATE VIEW mv_044 AS
SELECT
  COUNT(*) AS count_all
FROM time_dim AS t
INNER JOIN web_sales AS ws
  ON ws.ws_sold_time_sk = t.t_time_sk;
-- ============================================================
-- MV: mv_011
-- Fact Table: store_sales
-- Tables: household_demographics, store, store_sales, time_dim
-- Edges: household_demographics.hd_demo_sk=store_sales.ss_hdemo_sk, store.s_store_sk=store_sales.ss_store_sk, store_sales.ss_sold_time_sk=time_dim.t_time_sk
-- QBs: q88.sql::qb::subquery:1::root.from.subquery1, q88.sql::qb::subquery:2::root.join.subquery2, q88.sql::qb::subquery:3::root.join.subquery3, q88.sql::qb::subquery:4::root.join.subquery4, q88.sql::qb::subquery:5::root.join.subquery5, ... (9 total)
-- Columns: 11 columns
-- ============================================================
CREATE VIEW mv_011 AS
SELECT
  COUNT(*) AS count_all
FROM household_demographics AS hd
INNER JOIN store_sales AS ss
  ON ss.ss_hdemo_sk = hd.hd_demo_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
INNER JOIN time_dim AS t
  ON t.t_time_sk = ss.ss_sold_time_sk;
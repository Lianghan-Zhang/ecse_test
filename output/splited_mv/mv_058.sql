-- ============================================================
-- MV: mv_058
-- Fact Table: catalog_sales
-- Tables: call_center, catalog_sales
-- Edges: call_center.cc_call_center_sk=catalog_sales.cs_call_center_sk
-- QBs: q57.sql::qb::cte:v1::root.with.v1, q99.sql::qb::main:0::root
-- Columns: 9 columns
-- ============================================================
CREATE VIEW mv_058 AS
SELECT
  cc.cc_name,
  SUM(cs.cs_sales_price) AS sum_cs__cs_sales_price
FROM call_center AS cc
INNER JOIN catalog_sales AS cs
  ON cs.cs_call_center_sk = cc.cc_call_center_sk
GROUP BY
  cc.cc_name;
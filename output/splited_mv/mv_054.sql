-- ============================================================
-- MV: mv_054
-- Fact Table: catalog_sales
-- Tables: catalog_sales, ship_mode, warehouse
-- Edges: catalog_sales.cs_ship_mode_sk=ship_mode.sm_ship_mode_sk, catalog_sales.cs_warehouse_sk=warehouse.w_warehouse_sk
-- QBs: q66.sql::qb::union_branch:2::root.from.subquery1.union.right, q99.sql::qb::main:0::root
-- Columns: 19 columns
-- ============================================================
CREATE VIEW mv_054 AS
SELECT
  sm.sm_type,
  w.w_city,
  w.w_country,
  w.w_county,
  w.w_state,
  w.w_warehouse_name,
  w.w_warehouse_sq_ft
FROM catalog_sales AS cs
INNER JOIN ship_mode AS sm
  ON sm.sm_ship_mode_sk = cs.cs_ship_mode_sk
INNER JOIN warehouse AS w
  ON w.w_warehouse_sk = cs.cs_warehouse_sk
GROUP BY
  sm.sm_type,
  w.w_city,
  w.w_country,
  w.w_county,
  w.w_state,
  w.w_warehouse_name,
  w.w_warehouse_sq_ft;
-- ============================================================
-- MV: mv_041
-- Fact Table: web_sales
-- Tables: ship_mode, warehouse, web_sales
-- Edges: ship_mode.sm_ship_mode_sk=web_sales.ws_ship_mode_sk, warehouse.w_warehouse_sk=web_sales.ws_warehouse_sk
-- QBs: q62.sql::qb::main:0::root, q66.sql::qb::union_branch:1::root.from.subquery1.union.left
-- Columns: 19 columns
-- ============================================================
CREATE VIEW mv_041 AS
SELECT
  sm.sm_type,
  w.w_city,
  w.w_country,
  w.w_county,
  w.w_state,
  w.w_warehouse_name,
  w.w_warehouse_sq_ft
FROM ship_mode AS sm
INNER JOIN web_sales AS ws
  ON ws.ws_ship_mode_sk = sm.sm_ship_mode_sk
INNER JOIN warehouse AS w
  ON w.w_warehouse_sk = ws.ws_warehouse_sk
GROUP BY
  sm.sm_type,
  w.w_city,
  w.w_country,
  w.w_county,
  w.w_state,
  w.w_warehouse_name,
  w.w_warehouse_sq_ft;
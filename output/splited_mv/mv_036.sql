-- ============================================================
-- MV: mv_036
-- Fact Table: web_sales
-- Tables: customer_address, date_dim, web_sales, web_site
-- Edges: customer_address.ca_address_sk=ws1.ws_ship_addr_sk, date_dim.d_date_sk=ws1.ws_ship_date_sk, web_site.web_site_sk=ws1.ws_web_site_sk
-- QBs: q94.sql::qb::main:0::root, q95.sql::qb::main:0::root
-- Columns: 13 columns
-- ============================================================
CREATE VIEW mv_036 AS
SELECT
  SUM(web_sales.ws_ext_ship_cost) AS `total shipping cost `,
  SUM(web_sales.ws_net_profit) AS `total net profit `
FROM customer_address AS ca
INNER JOIN web_sales AS ws1
  ON ws1.ws_ship_addr_sk = ca.ca_address_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = ws1.ws_ship_date_sk
INNER JOIN web_site AS web
  ON web.web_site_sk = ws1.ws_web_site_sk;
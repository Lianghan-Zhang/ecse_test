-- ============================================================
-- MV: mv_035
-- Fact Table: web_sales
-- Tables: customer_address, date_dim, item, web_sales
-- Edges: customer_address.ca_address_sk=web_sales.ws_bill_addr_sk, date_dim.d_date_sk=web_sales.ws_sold_date_sk, item.i_item_sk=web_sales.ws_item_sk
-- QBs: q31.sql::qb::cte:ws::root.with.ws, q33.sql::qb::cte:ws::root.with.ws, q56.sql::qb::cte:ws::root.with.ws, q60.sql::qb::cte:ws::root.with.ws
-- Columns: 14 columns
-- ============================================================
CREATE VIEW mv_035 AS
SELECT
  ca.ca_county,
  d.d_qoy,
  d.d_year,
  i.i_item_id,
  i.i_manufact_id,
  SUM(ws.ws_ext_sales_price) AS sum_ws__ws_ext_sales_price
FROM customer_address AS ca
INNER JOIN web_sales AS ws
  ON ws.ws_bill_addr_sk = ca.ca_address_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = ws.ws_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ws.ws_item_sk
GROUP BY
  ca.ca_county,
  d.d_qoy,
  d.d_year,
  i.i_item_id,
  i.i_manufact_id;
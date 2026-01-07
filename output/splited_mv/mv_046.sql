-- ============================================================
-- MV: mv_046
-- Fact Table: catalog_sales
-- Tables: catalog_sales, date_dim, item, store, store_returns, store_sales
-- Edges: catalog_sales.cs_bill_customer_sk=store_returns.sr_customer_sk, catalog_sales.cs_item_sk=store_returns.sr_item_sk, catalog_sales.cs_sold_date_sk=d3.d_date_sk, ... (10 total)
-- QBs: q17.sql::qb::main:0::root, q25.sql::qb::main:0::root, q29.sql::qb::main:0::root
-- Columns: 37 columns
-- ============================================================
CREATE VIEW mv_046 AS
SELECT
  i.i_item_desc,
  i.i_item_id,
  s.s_state,
  s.s_store_id,
  s.s_store_name,
  COUNT(ss.ss_quantity) AS store_sales_quantitycount,
  AVG(ss.ss_quantity) AS avg_ss__ss_quantity,
  COUNT(sr.sr_return_quantity) AS as_store_returns_quantitycount,
  AVG(sr.sr_return_quantity) AS avg_sr__sr_return_quantity,
  COUNT(cs.cs_quantity) AS catalog_sales_quantitycount,
  AVG(cs.cs_quantity) AS avg_cs__cs_quantity,
  SUM(ss.ss_quantity) AS store_sales_quantity,
  SUM(sr.sr_return_quantity) AS store_returns_quantity,
  SUM(cs.cs_quantity) AS catalog_sales_quantity,
  SUM(ss.ss_net_profit) AS store_sales_profit,
  SUM(sr.sr_net_loss) AS store_returns_loss,
  SUM(cs.cs_net_profit) AS catalog_sales_profit
FROM catalog_sales AS cs
INNER JOIN date_dim AS d3
  ON d3.d_date_sk = cs.cs_sold_date_sk
INNER JOIN store_returns AS sr
  ON sr.sr_item_sk = cs.cs_item_sk
INNER JOIN date_dim AS d2
  ON d2.d_date_sk = sr.sr_returned_date_sk
INNER JOIN store_sales AS ss
  ON ss.ss_ticket_number = sr.sr_ticket_number
INNER JOIN date_dim AS d1
  ON d1.d_date_sk = ss.ss_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  i.i_item_desc,
  i.i_item_id,
  s.s_state,
  s.s_store_id,
  s.s_store_name;
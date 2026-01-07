-- ============================================================
-- MV: mv_061
-- Fact Table: web_returns
-- Tables: date_dim, item, web_returns
-- Edges: date_dim.d_date_sk=web_returns.wr_returned_date_sk, item.i_item_sk=web_returns.wr_item_sk
-- QBs: q30.sql::qb::cte:customer_total_return::root.with.customer_total_return, q77.sql::qb::cte:wr::root.with.wr, q83.sql::qb::cte:wr_items::root.with.wr_items
-- Columns: 13 columns
-- ============================================================
CREATE VIEW mv_061 AS
SELECT
  i.i_item_id,
  wr.wr_returning_customer_sk,
  SUM(wr.wr_return_amt) AS sum_wr__wr_return_amt,
  SUM(wr.wr_net_loss) AS profit_loss,
  SUM(wr.wr_return_quantity) AS wr_item_qty
FROM date_dim AS d
INNER JOIN web_returns AS wr
  ON wr.wr_returned_date_sk = d.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = wr.wr_item_sk
GROUP BY
  i.i_item_id,
  wr.wr_returning_customer_sk;
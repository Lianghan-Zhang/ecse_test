-- ============================================================
-- MV: mv_031
-- Fact Table: store_returns
-- Tables: date_dim, item, store_returns
-- Edges: date_dim.d_date_sk=store_returns.sr_returned_date_sk, item.i_item_sk=store_returns.sr_item_sk
-- QBs: q1.sql::qb::cte:customer_total_return::root.with.customer_total_return, q77.sql::qb::cte:sr::root.with.sr, q83.sql::qb::cte:sr_items::root.with.sr_items
-- Columns: 12 columns
-- ============================================================
CREATE VIEW mv_031 AS
SELECT
  i.i_item_id,
  sr.sr_customer_sk,
  sr.sr_store_sk,
  SUM(sr.sr_return_amt) AS sum_sr__sr_return_amt,
  SUM(sr.sr_net_loss) AS profit_loss,
  SUM(sr.sr_return_quantity) AS sr_item_qty
FROM date_dim AS d
INNER JOIN store_returns AS sr
  ON sr.sr_returned_date_sk = d.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = sr.sr_item_sk
GROUP BY
  i.i_item_id,
  sr.sr_customer_sk,
  sr.sr_store_sk;
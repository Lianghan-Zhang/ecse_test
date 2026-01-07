-- ============================================================
-- MV: mv_063
-- Fact Table: catalog_returns
-- Tables: catalog_returns, date_dim, item
-- Edges: catalog_returns.cr_item_sk=item.i_item_sk, catalog_returns.cr_returned_date_sk=date_dim.d_date_sk
-- QBs: q77.sql::qb::cte:cr::root.with.cr, q81.sql::qb::cte:customer_total_return::root.with.customer_total_return, q83.sql::qb::cte:cr_items::root.with.cr_items
-- Columns: 13 columns
-- ============================================================
CREATE VIEW mv_063 AS
SELECT
  cr.cr_returning_customer_sk,
  i.i_item_id,
  SUM(cr.cr_return_amount) AS returns,
  SUM(cr.cr_net_loss) AS profit_loss,
  SUM(cr.cr_return_quantity) AS cr_item_qty,
  SUM(cr.cr_return_amt_inc_tax) AS ctr_total_return
FROM catalog_returns AS cr
INNER JOIN date_dim AS d
  ON d.d_date_sk = cr.cr_returned_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = cr.cr_item_sk
GROUP BY
  cr.cr_returning_customer_sk,
  i.i_item_id;
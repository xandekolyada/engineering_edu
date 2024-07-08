-- ClickHouse
CREATE TABLE deposits (
  id Int64,
  user_id Int64,
  amount Float64,
  created_at DateTime64
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE withdrawals (
  id Int64,
  user_id Int64,
  amount Float64,
  created_at DateTime64
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE date_agg_operations
(
  created_at Date,
  total_deposits Float64,
  total_withdrawals Float64,
  total_revenue Float64
) ENGINE = SummingMergeTree
ORDER BY created_at;

CREATE MATERIALIZED VIEW date_agg_operations_mv TO date_agg_operations AS
SELECT
  created_at,
  ifNull(total_deposits, 0) AS total_deposits,
  ifNull(total_withdrawals, 0) AS total_withdrawals,
  ifNull(total_deposits, 0) - ifNull(total_withdrawals, 0) AS total_revenue
FROM
(
  SELECT
    toDate(created_at) AS created_at,
    sum(amount) AS total_deposits
  FROM
    deposits
  GROUP BY
    toDate(created_at)
) d
FULL JOIN
(
  SELECT
    toDate(created_at) AS created_at,
    sum(amount) AS total_withdrawals
  FROM
    withdrawals
  GROUP BY
    toDate(created_at)
) w
ON d.created_at = w.created_at

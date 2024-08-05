-- ClickHouse
CREATE TABLE deposits (
  id Int64,
  user_id Int64,
  amount Float64,
  created_at DateTime64
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE withdrawals (
  id Int64,
  user_id Int64,
  amount Float64,
  created_at DateTime64
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE IF NOT EXISTS date_agg_operations
(
  created_at Date,
  total_deposits Float64,
  total_withdrawals Float64,
  total_revenue Float64
)
ENGINE = SummingMergeTree
ORDER BY created_at
AS
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
)
FULL JOIN
(
  SELECT
    toDate(created_at) AS created_at,
    sum(amount) AS total_withdrawals
  FROM
    withdrawals
  GROUP BY
    toDate(created_at)
)
USING (created_at)

CREATE MATERIALIZED VIEW deposits_mv TO date_agg_operations AS
SELECT
  toDate(created_at) AS created_at,
  sum(amount) AS total_deposits,
  0 AS total_withdrawals,
  total_deposits AS total_revenue
FROM
  deposits
GROUP BY
  toDate(created_at)

CREATE MATERIALIZED VIEW withdrawals_mv TO date_agg_operations AS
SELECT
  toDate(created_at) AS created_at,
  0 AS total_deposits,
  sum(amount) AS total_withdrawals,
  -total_withdrawals AS total_revenue
FROM
  withdrawals
GROUP BY
  toDate(created_at)

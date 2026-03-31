/* @bruin

name: mart.customer_retention
type: bigquery.sql
materialization:
  type: table

depends:
  - staging.orders
  - raw.ingest_olist_raw

columns:
  - name: customer_unique_id
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: first_purchase_ts_utc
    type: timestamp
    checks:
      - name: not_null
  - name: second_purchase_ts_utc
    type: timestamp
  - name: days_to_second_purchase
    type: float64
  - name: has_second_purchase
    type: boolean
    checks:
      - name: not_null

@bruin */

WITH c AS (
  SELECT customer_id, customer_unique_id
  FROM raw.customers
),
orders AS (
  SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_ts_utc
  FROM staging.orders o
  WHERE o.order_purchase_ts_utc IS NOT NULL
    AND o.order_status IN ('delivered','shipped','invoiced','processing','approved')
),
ordered AS (
  SELECT
    c.customer_unique_id,
    o.order_purchase_ts_utc,
    row_number() OVER (PARTITION BY c.customer_unique_id ORDER BY o.order_purchase_ts_utc) AS rn
  FROM orders o
  JOIN c
    ON o.customer_id = c.customer_id
),
pivoted AS (
  SELECT
    customer_unique_id,
    MAX(CASE WHEN rn = 1 THEN order_purchase_ts_utc END) AS first_purchase_ts_utc,
    MAX(CASE WHEN rn = 2 THEN order_purchase_ts_utc END) AS second_purchase_ts_utc
  FROM ordered
  GROUP BY 1
)
SELECT
  customer_unique_id,
  first_purchase_ts_utc,
  second_purchase_ts_utc,
  CASE
    WHEN second_purchase_ts_utc IS NULL THEN NULL
    ELSE DATE_DIFF(, , DAY) * 1.0
  END AS days_to_second_purchase,
  (second_purchase_ts_utc IS NOT NULL) AS has_second_purchase
FROM pivoted


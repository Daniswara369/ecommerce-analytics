/* @bruin

name: mart.geo_delivery_delay
type: bigquery.sql
materialization:
  type: table

depends:
  - staging.orders
  - raw.ingest_olist_raw

columns:
  - name: customer_state
    type: string
    checks:
      - name: not_null
  - name: avg_delay_days
    type: float64
    checks:
      - name: not_null
  - name: delivered_orders
    type: integer
    checks:
      - name: not_null
      - name: positive

@bruin */

WITH delivered AS (
  SELECT
    o.order_id,
    c.customer_state,
    DATE_DIFF(o.order_delivered_customer_ts_utc, o.order_estimated_delivery_ts_utc, DAY) * 1.0 AS delay_days
  FROM staging.orders o
  JOIN raw.customers c
    ON o.customer_id = c.customer_id
  WHERE o.order_status = 'delivered'
    AND o.order_estimated_delivery_ts_utc IS NOT NULL
    AND o.order_delivered_customer_ts_utc IS NOT NULL
)
SELECT
  customer_state,
  avg(delay_days) AS avg_delay_days,
  count(*) AS delivered_orders
FROM delivered
GROUP BY 1

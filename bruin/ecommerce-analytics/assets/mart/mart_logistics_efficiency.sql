/* @bruin

name: mart.logistics_efficiency
type: bigquery.sql
materialization:
  type: table

depends:
  - staging.orders
  - staging.order_items

columns:
  - name: order_id
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: delivery_days_actual
    type: float64
    checks:
      - name: positive
  - name: delivery_days_estimated
    type: float64
    checks:
      - name: positive
  - name: latency_days
    type: float64
  - name: latency_score
    type: float64
  - name: freight_ratio
    type: float64
    checks:
      - name: positive

custom_checks:
  - name: delivery_time outliers (3 sigma) must be absent
    description: Fails if the mart still contains rows above the raw delivered baseline (mean + 3*stddev).
    query: |
      WITH delivered_raw AS (
        SELECT
          DATE_DIFF(o.order_delivered_customer_ts_utc, o.order_purchase_ts_utc, DAY) * 1.0 AS delivery_days_actual
        FROM staging.orders o
        WHERE o.order_status = 'delivered'
          AND o.order_purchase_ts_utc IS NOT NULL
          AND o.order_delivered_customer_ts_utc IS NOT NULL
      ),
      baseline AS (
        SELECT avg(delivery_days_actual) AS mu, STDDEV_SAMP(delivery_days_actual) AS sigma
        FROM delivered_raw
        WHERE delivery_days_actual IS NOT NULL
      )
      SELECT
        SUM(CASE WHEN m.delivery_days_actual > (b.mu + 3*b.sigma) THEN 1 ELSE 0 END) = 0
      FROM mart.logistics_efficiency m
      CROSS JOIN baseline b
    value: 1

@bruin */

WITH delivered AS (
  SELECT
    o.order_id,
    o.customer_id,
    o.order_purchase_ts_utc,
    o.order_delivered_customer_ts_utc,
    o.order_estimated_delivery_ts_utc
  FROM staging.orders o
  WHERE o.order_status = 'delivered'
),
order_item_rollup AS (
  SELECT
    order_id,
    SUM(price) AS items_price_total,
    SUM(freight_value) AS freight_total
  FROM staging.order_items
  GROUP BY 1
),
scored AS (
  SELECT
    d.order_id,
    d.customer_id,
    d.order_purchase_ts_utc,
    d.order_delivered_customer_ts_utc,
    d.order_estimated_delivery_ts_utc,
    DATE_DIFF(d.order_delivered_customer_ts_utc, d.order_purchase_ts_utc, DAY) * 1.0 AS delivery_days_actual,
    DATE_DIFF(d.order_estimated_delivery_ts_utc, d.order_purchase_ts_utc, DAY) * 1.0 AS delivery_days_estimated,
    DATE_DIFF(d.order_delivered_customer_ts_utc, d.order_estimated_delivery_ts_utc, DAY) * 1.0 AS latency_days,
    CASE
      WHEN DATE_DIFF(d.order_estimated_delivery_ts_utc, d.order_purchase_ts_utc, DAY) <= 0 THEN NULL
      ELSE 1.0 - (GREATEST(0, DATE_DIFF(d.order_delivered_customer_ts_utc, d.order_estimated_delivery_ts_utc, DAY))
                  / (DATE_DIFF(d.order_estimated_delivery_ts_utc, d.order_purchase_ts_utc, DAY) * 1.0))
    END AS latency_score,
    CASE
      WHEN r.items_price_total <= 0 THEN NULL
      ELSE r.freight_total / r.items_price_total
    END AS freight_ratio,
    r.items_price_total,
    r.freight_total
  FROM delivered d
  LEFT JOIN order_item_rollup r
    ON d.order_id = r.order_id
),
stats AS (
  SELECT
    avg(delivery_days_actual) AS mu,
    STDDEV_SAMP(delivery_days_actual) AS sigma
  FROM scored
  WHERE delivery_days_actual IS NOT NULL
)
SELECT
  s.*
FROM scored s
CROSS JOIN stats
WHERE s.delivery_days_actual IS NULL
   OR stats.sigma IS NULL
   OR s.delivery_days_actual <= (stats.mu + 3 * stats.sigma)

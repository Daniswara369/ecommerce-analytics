/* @bruin

name: mart.seller_performance
type: bigquery.sql
materialization:
  type: table

depends:
  - staging.orders
  - staging.order_items
  - raw.ingest_olist_raw

columns:
  - name: seller_id
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: avg_review_score
    type: float64
  - name: avg_delivery_days
    type: float64
  - name: fulfillment_speed_rank
    type: integer
  - name: review_score_rank
    type: integer

@bruin */

WITH reviews AS (
  SELECT
    r.order_id,
    CAST(r.review_score AS FLOAT64) AS review_score
  FROM raw.order_reviews r
),
delivered AS (
  SELECT
    o.order_id,
    o.order_purchase_ts_utc,
    o.order_delivered_customer_ts_utc
  FROM staging.orders o
  WHERE o.order_status = 'delivered'
    AND o.order_delivered_customer_ts_utc IS NOT NULL
    AND o.order_purchase_ts_utc IS NOT NULL
),
seller_orders AS (
  SELECT DISTINCT
    i.seller_id,
    i.order_id
  FROM staging.order_items i
),
joined AS (
  SELECT
    so.seller_id,
    d.order_id,
    DATE_DIFF(, , DAY) * 1.0 AS delivery_days,
    rv.review_score
  FROM seller_orders so
  JOIN delivered d
    ON so.order_id = d.order_id
  LEFT JOIN reviews rv
    ON d.order_id = rv.order_id
)
SELECT
  seller_id,
  avg(review_score) AS avg_review_score,
  avg(delivery_days) AS avg_delivery_days,
  dense_rank() OVER (ORDER BY avg_delivery_days ASC NULLS LAST) AS fulfillment_speed_rank,
  dense_rank() OVER (ORDER BY avg_review_score DESC NULLS LAST) AS review_score_rank,
  count(*) AS delivered_orders
FROM joined
GROUP BY 1


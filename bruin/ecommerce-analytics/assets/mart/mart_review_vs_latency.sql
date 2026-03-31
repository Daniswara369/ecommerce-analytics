/* @bruin

name: mart.review_vs_latency
type: bigquery.sql
materialization:
  type: table

depends:
  - mart.logistics_efficiency
  - raw.ingest_olist_raw

columns:
  - name: order_id
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: review_score
    type: float64
  - name: latency_days
    type: float64
    checks:
      - name: not_null

@bruin */

SELECT
  l.order_id,
  avg(CAST(r.review_score AS FLOAT64)) AS review_score,
  l.latency_days
FROM mart.logistics_efficiency l
LEFT JOIN raw.order_reviews r
  ON l.order_id = r.order_id
WHERE l.latency_days IS NOT NULL
GROUP BY 1, 3


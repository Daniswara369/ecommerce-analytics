/* @bruin

name: mart.order_status_flow
type: bigquery.sql
materialization:
  type: table

depends:
  - staging.orders

columns:
  - name: source_status
    type: string
    checks:
      - name: not_null
  - name: target_status
    type: string
    checks:
      - name: not_null
  - name: order_count
    type: integer
    checks:
      - name: not_null
      - name: positive

@bruin */

WITH latest AS (
  SELECT
    order_id,
    order_status
  FROM staging.orders
)
SELECT
  'purchased' AS source_status,
  'shipped' AS target_status,
  SUM(CASE WHEN order_status IN ('shipped', 'delivered') THEN 1 ELSE 0 END) AS order_count
FROM latest

UNION ALL

SELECT
  'shipped' AS source_status,
  'delivered' AS target_status,
  SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) AS order_count
FROM latest


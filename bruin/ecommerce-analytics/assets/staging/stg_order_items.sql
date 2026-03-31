/* @bruin

name: staging.order_items
type: bigquery.sql
materialization:
  type: view

depends:
  - raw.ingest_olist_raw
  - staging.orders

columns:
  - name: order_id
    type: string
    checks:
      - name: not_null
  - name: order_item_id
    type: integer
    checks:
      - name: not_null
      - name: positive
  - name: product_id
    type: string
    checks:
      - name: not_null
  - name: seller_id
    type: string
    checks:
      - name: not_null
  - name: price
    type: float64
    checks:
      - name: not_null
      - name: positive
  - name: freight_value
    type: float64
    checks:
      - name: not_null
      - name: positive

custom_checks:
  - name: freight cannot exceed 100% of item price
    description: Flags extreme freight pricing; should be <= item price for almost all items.
    query: |
      SELECT
        SUM(CASE WHEN freight_value > price THEN 1 ELSE 0 END) = 0
      FROM staging.order_items
    value: 1
  - name: all order_item rows map to orders
    description: Every order_id in items should exist in the orders table.
    query: |
      SELECT
        COUNT(*) = 0
      FROM staging.order_items i
      LEFT JOIN staging.orders o
        ON i.order_id = o.order_id
      WHERE o.order_id IS NULL
    value: 1

@bruin */

SELECT
  order_id,
  CAST(order_item_id AS INTEGER) AS order_item_id,
  product_id,
  seller_id,
  shipping_limit_date,
  CAST(price AS FLOAT64) AS price,
  CAST(freight_value AS FLOAT64) AS freight_value
FROM raw.order_items
WHERE CAST(price AS FLOAT64) > 0
  AND CAST(freight_value AS FLOAT64) > 0
  AND CAST(freight_value AS FLOAT64) <= CAST(price AS FLOAT64)


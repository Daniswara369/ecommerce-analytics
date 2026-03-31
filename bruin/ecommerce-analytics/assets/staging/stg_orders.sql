/* @bruin

name: staging.orders
type: bigquery.sql
materialization:
  type: view

depends:
  - raw.ingest_olist_raw

columns:
  - name: order_id
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: order_status
    type: string
    checks:
      - name: not_null
      - name: accepted_values
        value: ["created","approved","invoiced","processing","shipped","delivered","canceled","unavailable"]
  - name: order_purchase_ts_utc
    type: timestamp
    checks:
      - name: not_null

custom_checks:
  - name: delivered orders must have delivered timestamp
    description: Delivered orders should have a delivered timestamp populated.
    query: |
      SELECT
        SUM(CASE WHEN order_status = 'delivered' AND order_delivered_customer_ts_utc IS NULL THEN 1 ELSE 0 END) = 0
      FROM staging.orders
    value: 1

@bruin */

WITH typed AS (
  SELECT
    order_id,
    customer_id,
    order_status,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(order_purchase_timestamp AS STRING)) AS order_purchase_ts,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(order_approved_at AS STRING)) AS order_approved_ts,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(order_delivered_carrier_date AS STRING)) AS order_delivered_carrier_ts,
    NULLIF(CAST(order_delivered_customer_date AS STRING), '') AS order_delivered_customer_date_clean,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', NULLIF(CAST(order_delivered_customer_date AS STRING), '')) AS order_delivered_customer_ts,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(order_estimated_delivery_date AS STRING)) AS order_estimated_delivery_ts
  FROM raw.orders
),
utc AS (
  SELECT
    *,
    -- BigQuery uses simpler AT TIME ZONE syntax
    DATETIME(order_purchase_ts, 'America/Sao_Paulo') AS order_purchase_lt,
    DATETIME(order_approved_ts, 'America/Sao_Paulo') AS order_approved_lt,
    DATETIME(order_delivered_carrier_ts, 'America/Sao_Paulo') AS order_delivered_carrier_lt,
    DATETIME(order_delivered_customer_ts, 'America/Sao_Paulo') AS order_delivered_customer_lt,
    DATETIME(order_estimated_delivery_ts, 'America/Sao_Paulo') AS order_estimated_delivery_lt
  FROM typed
)
SELECT
  order_id,
  customer_id,
  order_status,
  order_purchase_ts AS order_purchase_ts_utc,
  order_approved_ts AS order_approved_ts_utc,
  order_delivered_carrier_ts AS order_delivered_carrier_ts_utc,
  CASE
    WHEN order_status = 'delivered' AND order_delivered_customer_ts IS NULL
      THEN order_estimated_delivery_ts
    ELSE order_delivered_customer_ts
  END AS order_delivered_customer_ts_utc,
  order_estimated_delivery_ts AS order_estimated_delivery_ts_utc
FROM typed

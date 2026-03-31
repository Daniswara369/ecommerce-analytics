/* @bruin

name: raw.tables_ready
type: bigquery.sql
materialization:
  type: view

depends:
  - raw.ingest_olist_raw

custom_checks:
  - name: raw orders exist
    description: Ensures ingestion created the raw.orders table.
    query: |
      SELECT COUNT(*) = 1
      FROM `ecommerce-analytics-491918.raw.__TABLES__`
      WHERE table_id = 'orders'
    value: 1

@bruin */

SELECT 1 AS ok


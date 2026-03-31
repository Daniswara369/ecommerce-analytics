/* @bruin

name: staging.products
type: bigquery.sql
materialization:
  type: view

depends:
  - raw.ingest_olist_raw

columns:
  - name: product_id
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: product_category_name_en
    type: string
@bruin */

SELECT
  p.product_id,
  p.product_category_name,
  coalesce(t.product_category_name_english, p.product_category_name) AS product_category_name_en,
  p.product_name_lenght,
  p.product_description_lenght,
  p.product_photos_qty,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm
FROM raw.products p
LEFT JOIN raw.product_category_name_translation t
  ON p.product_category_name = t.product_category_name


# Olist Logistics Performance Dashboard

```sql sankey_data
SELECT
  source_status AS source,
  target_status AS target,
  order_count AS value
FROM olist.order_status_flow
ORDER BY 1, 2
```

```sql geo_delay
SELECT
  customer_state,
  avg_delay_days,
  delivered_orders
FROM olist.geo_delivery_delay
ORDER BY avg_delay_days DESC
```

```sql review_latency
SELECT
  order_id,
  review_score,
  latency_days
FROM olist.review_vs_latency
WHERE review_score IS NOT NULL
```

<br/>

## Order State Progression
<SankeyDiagram data={sankey_data} sourceCol="source" targetCol="target" valueCol="value" />

## Average Delivery Delay by State
<BarChart data={geo_delay} x="customer_state" y="avg_delay_days" swapXY=true title="Impact of geography on fulfillment" />

## Delivery Speed vs. Review Score
<ScatterPlot data={review_latency} x="latency_days" y="review_score" title="Is faster always better?" />

<style>
  :global(body) {
    background-color: #0f172a !important; /* slate-900 */
    color: #e2e8f0 !important; /* slate-200 */
    font-family: "Inter", system-ui, sans-serif !important;
  }
  
  :global(h1), :global(h2), :global(h3) {
    color: #f8fafc !important; /* slate-50 */
    font-weight: 700;
  }

  /* Glassmorphism for charts or general containers */
  :global(.evidence-chart), :global(.echarts-for-react) {
    background: rgba(30, 41, 59, 0.7) !important; /* slate-800 translucent */
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
    border-radius: 12px !important;
    padding: 16px !important;
    box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3) !important;
    backdrop-filter: blur(10px) !important;
    transition: transform 0.2s ease, box-shadow 0.2s ease !important;
  }
  
  :global(.evidence-chart:hover) {
    transform: translateY(-2px) !important;
    box-shadow: 0 12px 40px 0 rgba(0, 0, 0, 0.4) !important;
  }

  /* Target Evidence default text/SVG inside charts to switch to light colors */
  :global(svg text) {
    fill: #cbd5e1 !important; /* slate-300 */
  }
  
  :global(.evidence-table) {
    background: rgba(30, 41, 59, 0.5) !important;
    color: #cbd5e1 !important;
  }
</style>


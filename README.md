# Ecommerce Analytics Pipeline — Olist Delivery Engine

"In the Brazilian e-commerce market, delivery speed makes or breaks customer trust."

This pipeline ingests, transforms, and analyzes over 100k anonymous orders from Olist Store — built with Bruin and visualized with Evidence.dev — to uncover customer lifetime value, logistics performance, and seller fulfillment efficiency. 

## 🏆 Key Findings
- 🚚 **Delivery Speed predicts Satisfaction**: Fast delivery times strongly correlate with 5-star reviews, whereas delayed items plummet in satisfaction.
- 🗺️ **Regional bottlenecks**: More remote northern states experience average delivery delays over twice as long as southern regions due to geographic distance.
- 🔄 **Order flow health**: The vast majority of orders successfully reach 'delivered' status, with only a tiny fraction dropping at 'canceled' or 'unavailable'.

## 🗺️ Architecture
```text
Kaggle CSV (olist_orders.csv)
      │
      ▼
┌────────────────────────┐
│ raw.olist_tables       │ ← Seed asset — 100k+ orders ingested
│ (BigQuery)             │
└──────────┬─────────────┘
           │
           ▼
┌────────────────────────┐
│ staging.orders         │ ← Cleaned, typed, quality checks
│ (BigQuery)             │
└────┬────────────┬──────┘
     │            │       
     ▼            ▼       
┌─────────┐  ┌──────────┐ 
│ mart.   │  │ mart.    │ 
│ flow    │  │ delay    │ 
│         │  │          │  
└─────────┘  └──────────┘  
     │            │        
     ▼            ▼        
┌────────────────────────┐
│ Evidence.dev           │ ← Interactive single-page dashboard
│ Dashboard              │   Charts + Logistics visualizations
└────────────────────────┘
```

## 🛠️ Tech Stack
- [Bruin](https://getbruin.com)
- [BigQuery](https://cloud.google.com/bigquery)
- [Evidence.dev](https://evidence.dev)
- [GitHub Actions](https://github.com/features/actions)

- **Source**: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **Size**: ~100k orders

### Layer 1 — Raw
Ingests the Olist CSV files directly into Python dicts or raw DB. Preserves the source structure intact.

### Layer 2 — Staging
Cleans and standardizes the raw data:
- Joins orders with reviews and geolocations
- Standardizes timestamps and resolves string types
- Filters invalid or corrupted state tags

### Layer 3 — Mart
Modelled for final analytics in BigQuery:
- `mart.order_status_flow`: Aggregated stages of order statuses.
- `mart.geo_delivery_delay`: Delivery latency statistics bucketed by state.
- `mart.review_vs_latency`: Micro-level relation between speed and customer satisfaction.

## ✅ Data Quality
Built-in tests validating primary keys, ensuring positive delivery latencies (no time-travel anomalies), and verifying valid review constraints. 

## 🔄 CI/CD with GitHub Actions
The pipeline validates automatically on every push to main via `.github/workflows/bruin-pipeline.yml`. 

### Prerequisites
- Bruin CLI
- Node.js (for Evidence.dev dashboard)
- Google Cloud Service Account JSON key

### 1. Ingest Data
```powershell
# Navigate to pipeline dir
cd bruin/ecommerce-analytics

# Set BigQuery Credentials
$env:GOOGLE_CLOUD_PROJECT="your-project-id"
$env:GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account.json"

bruin run assets/ingest_olist_raw.py
```

### 2. Run Pipeline
```powershell
bruin run
```

### 3. Launch Dashboard
```powershell
# Navigate to dashboard dir
cd dashboard

# Initial source refresh
npm run sources

# Start visualization server
npm run dev
```

## 📁 Project Structure
```text
ecommerce-analytics/
├── README.md
├── .github/
│   └── workflows/
│       └── bruin-pipeline.yml
├── bruin/                  ← ELT Pipeline
│   └── ecommerce-analytics/
│       ├── .bruin.yml
│       └── assets/
│           ├── ingest_olist_raw.py
│           ├── staging/
│           └── mart/
└── dashboard/              ← Evidence.dev
    ├── pages/
    │   └── index.md
    └── sources/
        └── olist/          ← BigQuery connection
            ├── connection.yaml
            ├── connection.options.yaml
            └── *.sql
```

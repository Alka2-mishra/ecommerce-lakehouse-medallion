# E-Commerce Lakehouse Analytics | Medallion Architecture

A portfolio-ready starter project that ingests messy e-commerce order data, processes it with PySpark + Delta Lake through Bronze/Silver/Gold layers, and prepares Gold outputs for Power BI.

## Architecture

CSV source → Bronze (raw Delta) → Silver (validated/cleaned Delta) → Gold (business aggregates) → Power BI

## Stack
- Python + PySpark
- Delta Lake tables
- Medallion architecture
- SQL-friendly Gold tables
- Power BI for reporting

## Dataset
The project runs immediately on included synthetic sample CSVs. For a realistic portfolio version, use the public **Brazilian E-Commerce Public Dataset by Olist** from Kaggle:
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Download and extract the CSV files into `data/olist/`. The pipeline supports these Olist files:
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_products_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_order_payments_dataset.csv`

The included sample dataset is synthetic and is only for validating the pipeline end-to-end; do not present its metrics as real business findings.

## Quick start (local)
Requires Java compatible with your PySpark version and Python 3.10+.

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python src/run_pipeline.py --input data/sample --output lakehouse
```

To use Olist:
```bash
python src/run_pipeline.py --input data/olist --output lakehouse
```

The script writes Delta tables to `lakehouse/bronze`, `lakehouse/silver`, and `lakehouse/gold`. CSV exports for Power BI are written to `lakehouse/powerbi_exports/`.

> If your local Java/PySpark environment is troublesome, run the same transformations in a Databricks notebook or Microsoft Fabric notebook. Paths may need to be changed to the platform's Lakehouse/Files paths.

## Layers
### Bronze
- Reads source CSVs as strings where practical.
- Adds ingestion timestamp and source filename.
- Writes Delta tables, preserving source-level values.

### Silver
- Parses timestamps and numeric columns.
- Deduplicates order items.
- Validates positive price/freight and quantities.
- Joins order, item, product, customer and payment data.
- Keeps records with missing optional product attributes while flagging quality issues.

### Gold
- `gold_daily_sales`: daily orders, revenue, freight, payment total, average order value.
- `gold_category_sales`: product category performance.
- `gold_top_products`: top products by item revenue.
- `gold_customer_summary`: customer order and spend summary.

Revenue is defined as sum of item price. Order-level revenue is not added to payment totals; payment total is kept as a separate reconciliation metric to avoid double-counting.

## Power BI
1. Open Power BI Desktop.
2. Select **Get data → Text/CSV**.
3. Import the CSVs in `lakehouse/powerbi_exports/`.
4. Build visuals using `powerbi/dashboard_spec.md`.
5. For a cloud lakehouse deployment, connect Power BI to the platform's supported SQL endpoint/semantic model instead of exporting CSVs.

## Suggested dashboard
- KPI cards: Revenue, Orders, Average Order Value, Freight
- Line chart: Revenue by day/month
- Bar chart: Revenue by product category
- Bar chart: Top 10 products
- Table: Customer summary
- Slicers: Order date, category, order status

## Portfolio claims: be accurate
You can say you built a PySpark Medallion pipeline and prepared Gold analytics outputs. Only claim cloud deployment, scheduled orchestration, incremental processing, or a live Power BI connection after you have implemented and tested those parts.

## Repository structure
```text
ecommerce-lakehouse-project/
├── data/sample/                 # tiny synthetic demo input
├── src/run_pipeline.py          # Bronze → Silver → Gold pipeline
├── notebooks/                   # platform adaptation guide
├── powerbi/dashboard_spec.md    # report design
├── requirements.txt
└── README.md
```

## Next improvements
- Add unit tests for quality rules.
- Add incremental loads using `updated_at`/watermarks.
- Add pipeline logging and error quarantine.
- Add data quality metrics table.
- Publish Gold tables to Fabric/Databricks and connect Power BI.

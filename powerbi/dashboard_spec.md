# Power BI report specification

## Data imports
Import these generated files from `lakehouse/powerbi_exports/`:
- `gold_daily_sales.csv`
- `gold_category_sales.csv`
- `gold_top_products.csv`
- `gold_customer_summary.csv`

## Suggested pages
### Page 1 — Executive Overview
- Cards: `total_revenue`, `total_orders`, `average_order_value`, `total_freight`
- Line chart: `order_date` → `total_revenue`
- Bar chart: `category` → `total_revenue`
- Slicers: date, category

### Page 2 — Product Performance
- Bar chart: `product_id` → `item_revenue`, Top N = 10
- Table: product ID, category, item count, item revenue

### Page 3 — Customer Insights
- Table: customer ID, order count, customer spend
- Bar chart: order count by customer (Top N)

## Important
For a local CSV demo, refresh after rerunning the pipeline. A cloud-hosted Power BI connection requires publishing the Gold tables to a supported Fabric/Databricks endpoint and configuring credentials.

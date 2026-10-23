import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T
from delta import configure_spark_with_delta_pip


def create_spark():
    builder = (
        SparkSession.builder.appName("EcommerceMedallionPipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_csv_if_exists(spark, folder, filename):
    path = str(Path(folder) / filename)
    if not Path(path).exists():
        return None
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .csv(path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(filename))
    )


def first_existing(spark, folder, candidates):
    for filename in candidates:
        df = read_csv_if_exists(spark, folder, filename)
        if df is not None:
            return df
    return None


def write_delta(df, path):
    if df is not None:
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(path)))


def write_csv(df, path):
    if df is not None:
        (df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(path) + "_tmp"))
        # Spark writes a directory; keep that standard Spark output structure for reliable portability.


def safe_col(df, name, cast_type="string"):
    if name in df.columns:
        return F.col(name).cast(cast_type)
    return F.lit(None).cast(cast_type).alias(name)


def main(input_dir, output_dir):
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    inp, out = Path(input_dir), Path(output_dir)
    bronze_dir, silver_dir, gold_dir = out / "bronze", out / "silver", out / "gold"
    export_dir = out / "powerbi_exports"
    export_dir.mkdir(parents=True, exist_ok=True)

    # Recognize either the small included demo files or Olist public dataset filenames.
    orders = first_existing(spark, inp, ["orders.csv", "olist_orders_dataset.csv"])
    items = first_existing(spark, inp, ["order_items.csv", "olist_order_items_dataset.csv"])
    products = first_existing(spark, inp, ["products.csv", "olist_products_dataset.csv"])
    customers = first_existing(spark, inp, ["customers.csv", "olist_customers_dataset.csv"])
    payments = first_existing(spark, inp, ["payments.csv", "olist_order_payments_dataset.csv"])

    if orders is None or items is None:
        raise FileNotFoundError(
            f"Need orders.csv + order_items.csv or Olist orders/items CSVs in {inp}"
        )

    # BRONZE: raw-ish ingestion with source metadata.
    for name, df in [("orders", orders), ("order_items", items), ("products", products),
                     ("customers", customers), ("payments", payments)]:
        if df is not None:
            write_delta(df, bronze_dir / name)

    # Normalize Olist vs demo column names.
    order_id = "order_id"
    customer_id = "customer_id"
    order_date_col = "order_purchase_timestamp" if "order_purchase_timestamp" in orders.columns else "order_date"
    status_col = "order_status" if "order_status" in orders.columns else "status"
    item_order_col = "order_id"
    item_price_col = "price"
    item_product_col = "product_id"
    item_qty_col = "quantity" if "quantity" in items.columns else None

    orders_s = orders.select(
        safe_col(orders, order_id).alias("order_id"),
        safe_col(orders, customer_id).alias("customer_id"),
        F.to_timestamp(safe_col(orders, order_date_col)).alias("order_ts"),
        safe_col(orders, status_col).alias("order_status"),
    ).dropDuplicates(["order_id"])

    items_s = items.select(
        safe_col(items, item_order_col).alias("order_id"),
        safe_col(items, item_product_col).alias("product_id"),
        safe_col(items, "order_item_id", "int").alias("order_item_id"),
        safe_col(items, item_price_col, "double").alias("price"),
        safe_col(items, "freight_value", "double").alias("freight_value"),
        (safe_col(items, item_qty_col, "int") if item_qty_col else F.lit(1).cast("int")).alias("quantity"),
    )
    # For Olist, a row is identified by order_id + order_item_id. For demo, duplicate input rows are removed.
    dedupe_keys = ["order_id", "order_item_id"] if "order_item_id" in items.columns else ["order_id", "product_id", "price"]
    items_s = items_s.dropDuplicates(dedupe_keys)

    products_s = None
    if products is not None:
        cat_col = "product_category_name" if "product_category_name" in products.columns else "category"
        products_s = products.select(
            safe_col(products, "product_id").alias("product_id"),
            safe_col(products, cat_col).alias("category"),
        ).dropDuplicates(["product_id"])

    customers_s = None
    if customers is not None:
        cust_key = "customer_unique_id" if "customer_unique_id" in customers.columns else "customer_id"
        customers_s = customers.select(
            safe_col(customers, "customer_id").alias("customer_id"),
            safe_col(customers, cust_key).alias("customer_unique_id"),
        ).dropDuplicates(["customer_id"])

    payments_s = None
    if payments is not None:
        payments_s = payments.select(
            safe_col(payments, "order_id").alias("order_id"),
            safe_col(payments, "payment_value", "double").alias("payment_value"),
        ).groupBy("order_id").agg(F.sum("payment_value").alias("payment_total"))

    # SILVER: join order items to valid order headers, optional dimensions, and payment reconciliation.
    silver = items_s.join(orders_s, "order_id", "left")
    if products_s is not None:
        silver = silver.join(products_s, "product_id", "left")
    else:
        silver = silver.withColumn("category", F.lit("Unknown"))
    if customers_s is not None:
        silver = silver.join(customers_s, "customer_id", "left")
    if payments_s is not None:
        silver = silver.join(payments_s, "order_id", "left")
    else:
        silver = silver.withColumn("payment_total", F.lit(None).cast("double"))

    silver = (
        silver.withColumn("category", F.coalesce(F.col("category"), F.lit("Unknown")))
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("item_revenue", F.col("price") * F.coalesce(F.col("quantity"), F.lit(1)))
        .withColumn("quality_issue",
                    F.when(F.col("order_id").isNull(), "missing_order_id")
                    .when(F.col("price").isNull() | (F.col("price") <= 0), "invalid_price")
                    .when(F.col("order_ts").isNull(), "invalid_order_timestamp")
                    .otherwise("none"))
    )
    valid_silver = silver.filter(
        F.col("order_id").isNotNull() & F.col("price").isNotNull() & (F.col("price") > 0)
        & F.col("order_ts").isNotNull()
    )
    write_delta(silver, silver_dir / "order_items_enriched")
    write_delta(valid_silver, silver_dir / "order_items_valid")

    # GOLD: aggregated, business-facing outputs.
    daily = valid_silver.groupBy("order_date").agg(
        F.sum("item_revenue").alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum(F.coalesce("freight_value", F.lit(0.0))).alias("total_freight"),
        F.avg("item_revenue").alias("average_item_revenue"),
    ).withColumn(
        "average_order_value",
        F.when(F.col("total_orders") > 0, F.col("total_revenue") / F.col("total_orders"))
    ).orderBy("order_date")

    category = valid_silver.groupBy("category").agg(
        F.sum("item_revenue").alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.count("*").alias("item_count"),
    ).orderBy(F.desc("total_revenue"))

    top_products = valid_silver.groupBy("product_id", "category").agg(
        F.sum("item_revenue").alias("item_revenue"),
        F.count("*").alias("item_count"),
        F.countDistinct("order_id").alias("order_count"),
    ).orderBy(F.desc("item_revenue")).limit(20)

    customer_base = valid_silver
    customer_key = "customer_unique_id" if "customer_unique_id" in customer_base.columns else "customer_id"
    if customer_key in customer_base.columns:
        customer_summary = customer_base.groupBy(customer_key).agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("item_revenue").alias("customer_spend"),
            F.max("order_date").alias("last_order_date"),
        ).withColumnRenamed(customer_key, "customer_id").orderBy(F.desc("customer_spend"))
    else:
        customer_summary = None

    for name, df in [("gold_daily_sales", daily), ("gold_category_sales", category),
                     ("gold_top_products", top_products), ("gold_customer_summary", customer_summary)]:
        if df is not None:
            write_delta(df, gold_dir / name)
            # CSV is intentionally included for the simplest Power BI Desktop demo.
            (df.coalesce(1).write.mode("overwrite").option("header", True)
             .csv(str(export_dir / name)))

    # Export quality counts for the README/demo and data-quality discussion.
    quality = silver.groupBy("quality_issue").count()
    write_delta(quality, gold_dir / "gold_quality_summary")
    (quality.coalesce(1).write.mode("overwrite").option("header", True)
     .csv(str(export_dir / "gold_quality_summary")))

    print(f"Pipeline complete. Delta layers: {out.resolve()}")
    print(f"Power BI CSV export directories: {export_dir.resolve()}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-commerce Bronze/Silver/Gold pipeline")
    parser.add_argument("--input", default="data/sample", help="Folder containing source CSV files")
    parser.add_argument("--output", default="lakehouse", help="Output root directory")
    args = parser.parse_args()
    main(args.input, args.output)

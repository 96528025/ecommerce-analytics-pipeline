import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

OUTPUT_DIR = "output/parquet"

spark = (
    SparkSession.builder
    .appName("OlistETL")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.extraJavaOptions",
            "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# ── Extract ───────────────────────────────────────────────────────────────────

def extract():
    print("  Reading CSV files...")
    return {
        "customers":      spark.read.csv("olist_customers_dataset.csv",      header=True, inferSchema=True),
        "orders":         spark.read.csv("olist_orders_dataset.csv",          header=True, inferSchema=True),
        "order_items":    spark.read.csv("olist_order_items_dataset.csv",     header=True, inferSchema=True),
        "order_payments": spark.read.csv("olist_order_payments_dataset.csv",  header=True, inferSchema=True),
        "order_reviews":  spark.read.csv("olist_order_reviews_dataset.csv",   header=True, inferSchema=True, multiLine=True, escape='"'),
        "products":       spark.read.csv("olist_products_dataset.csv",        header=True, inferSchema=True),
        "sellers":        spark.read.csv("olist_sellers_dataset.csv",         header=True, inferSchema=True),
        "category_trans": spark.read.csv("product_category_name_translation.csv", header=True, inferSchema=True),
    }


# ── Transform ─────────────────────────────────────────────────────────────────

def build_dim_customers(customers):
    return (
        customers
        .select("customer_id", "customer_city", "customer_state")
        .dropDuplicates()
    )

def build_dim_products(products, category_trans):
    return (
        products
        .join(category_trans, on="product_category_name", how="left")
        .select(
            "product_id",
            F.col("product_category_name_english").alias("category_name"),
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        )
        .dropDuplicates()
    )

def build_dim_sellers(sellers):
    return (
        sellers
        .select("seller_id", "seller_city", "seller_state")
        .dropDuplicates()
    )

def build_dim_date(orders):
    return (
        orders
        .select(F.to_date("order_purchase_timestamp").alias("full_date"))
        .dropDuplicates()
        .dropna()
        .withColumn("date_key",    F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year",        F.year("full_date"))
        .withColumn("quarter",     F.quarter("full_date"))
        .withColumn("month",       F.month("full_date"))
        .withColumn("month_name",  F.date_format("full_date", "MMMM"))
        .withColumn("day",         F.dayofmonth("full_date"))
        .withColumn("weekday",     F.dayofweek("full_date"))
        .withColumn("weekday_name", F.date_format("full_date", "EEEE"))
    )

def build_fact_order_items(order_items, orders, order_payments, order_reviews,
                            dim_customers, dim_products, dim_sellers):
    payments = (
        order_payments
        .groupBy("order_id")
        .agg(F.sum("payment_value").alias("payment_value"))
    )
    reviews = (
        order_reviews
        .withColumn("review_score", F.col("review_score").cast("double"))
        .groupBy("order_id")
        .agg(F.avg("review_score").alias("review_score"))
    )

    return (
        order_items
        .join(orders.select("order_id", "customer_id", "order_purchase_timestamp"), on="order_id", how="left")
        .join(payments,  on="order_id", how="left")
        .join(reviews,   on="order_id", how="left")
        .join(dim_customers.select("customer_id"), on="customer_id", how="left")
        .join(dim_products.select("product_id"),   on="product_id",  how="left")
        .join(dim_sellers.select("seller_id"),     on="seller_id",   how="left")
        .withColumn("date_key", F.date_format(F.to_date("order_purchase_timestamp"), "yyyyMMdd").cast("int"))
        .select("order_id", "order_item_id", "customer_id", "product_id",
                "seller_id", "date_key", "price", "freight_value",
                "payment_value", "review_score")
        .dropna(subset=["customer_id", "product_id", "seller_id"])
    )


# ── Load (Parquet) ────────────────────────────────────────────────────────────

def save(df, name):
    path = f"{OUTPUT_DIR}/{name}"
    df.write.mode("overwrite").parquet(path)
    print(f"  ✓ {name}: {df.count():,} rows → {path}")


# ── Analytics ─────────────────────────────────────────────────────────────────

def run_analytics(fact, dim_products, dim_date):
    print("\n=== Spark SQL Analytics ===")

    fact.createOrReplaceTempView("fact_order_items")
    dim_products.createOrReplaceTempView("dim_products")
    dim_date.createOrReplaceTempView("dim_date")

    print("\n[1] Monthly GMV:")
    spark.sql("""
        SELECT d.year, d.month, d.month_name,
               COUNT(DISTINCT f.order_id) AS total_orders,
               ROUND(SUM(f.price), 2)     AS gmv
        FROM fact_order_items f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month
    """).show(30, truncate=False)

    print("\n[2] Top 5 categories by revenue:")
    spark.sql("""
        SELECT COALESCE(p.category_name, 'Unknown') AS category,
               COUNT(DISTINCT f.order_id)            AS orders,
               ROUND(SUM(f.price), 2)                AS revenue
        FROM fact_order_items f
        JOIN dim_products p ON f.product_id = p.product_id
        GROUP BY p.category_name
        ORDER BY revenue DESC
        LIMIT 5
    """).show(truncate=False)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=== Extract ===")
    dfs = extract()

    print("=== Transform ===")
    dim_customers = build_dim_customers(dfs["customers"])
    dim_products  = build_dim_products(dfs["products"], dfs["category_trans"])
    dim_sellers   = build_dim_sellers(dfs["sellers"])
    dim_date      = build_dim_date(dfs["orders"])
    fact          = build_fact_order_items(
        dfs["order_items"], dfs["orders"], dfs["order_payments"], dfs["order_reviews"],
        dim_customers, dim_products, dim_sellers
    )

    print("=== Load (Parquet) ===")
    save(dim_customers, "dim_customers")
    save(dim_products,  "dim_products")
    save(dim_sellers,   "dim_sellers")
    save(dim_date,      "dim_date")
    save(fact,          "fact_order_items")

    run_analytics(fact, dim_products, dim_date)

    spark.stop()
    print("\nDone!")


if __name__ == "__main__":
    main()

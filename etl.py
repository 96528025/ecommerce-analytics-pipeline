import pandas as pd
from sqlalchemy import create_engine

DB_URL = "postgresql://angelren@localhost:5432/olist_dw"
engine = create_engine(DB_URL)


# ── Extract ───────────────────────────────────────────────────────────────────

def extract():
    print("  Reading CSV files...")
    customers          = pd.read_csv("olist_customers_dataset.csv")
    orders             = pd.read_csv("olist_orders_dataset.csv")
    order_items        = pd.read_csv("olist_order_items_dataset.csv")
    order_payments     = pd.read_csv("olist_order_payments_dataset.csv")
    order_reviews      = pd.read_csv("olist_order_reviews_dataset.csv")
    products           = pd.read_csv("olist_products_dataset.csv")
    sellers            = pd.read_csv("olist_sellers_dataset.csv")
    category_trans     = pd.read_csv("product_category_name_translation.csv")
    return customers, orders, order_items, order_payments, order_reviews, products, sellers, category_trans


# ── Transform ─────────────────────────────────────────────────────────────────

def build_dim_customers(customers):
    dim = customers[["customer_id", "customer_city", "customer_state"]].drop_duplicates().reset_index(drop=True)
    dim.insert(0, "customer_key", range(1, len(dim) + 1))
    return dim


def build_dim_products(products, category_trans):
    dim = products.merge(category_trans, on="product_category_name", how="left")
    dim = dim[["product_id", "product_category_name_english",
               "product_weight_g", "product_length_cm",
               "product_height_cm", "product_width_cm"]].drop_duplicates().reset_index(drop=True)
    dim = dim.rename(columns={"product_category_name_english": "category_name"})
    dim.insert(0, "product_key", range(1, len(dim) + 1))
    return dim


def build_dim_sellers(sellers):
    dim = sellers[["seller_id", "seller_city", "seller_state"]].drop_duplicates().reset_index(drop=True)
    dim.insert(0, "seller_key", range(1, len(dim) + 1))
    return dim


def build_dim_date(orders):
    dates = pd.to_datetime(orders["order_purchase_timestamp"]).dt.normalize().dropna().unique()
    dim = pd.DataFrame({"full_date": sorted(dates)})
    dim["date_key"]    = dim["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim["year"]        = dim["full_date"].dt.year
    dim["quarter"]     = dim["full_date"].dt.quarter
    dim["month"]       = dim["full_date"].dt.month
    dim["month_name"]  = dim["full_date"].dt.strftime("%B")
    dim["day"]         = dim["full_date"].dt.day
    dim["weekday"]     = dim["full_date"].dt.dayofweek
    dim["weekday_name"] = dim["full_date"].dt.strftime("%A")
    return dim


def build_fact_order_items(order_items, orders, order_payments, order_reviews,
                            dim_customers, dim_products, dim_sellers):
    payments = order_payments.groupby("order_id")["payment_value"].sum().reset_index()
    reviews  = order_reviews.groupby("order_id")["review_score"].mean().reset_index()

    fact = order_items.merge(
        orders[["order_id", "customer_id", "order_purchase_timestamp"]], on="order_id", how="left"
    )
    fact = fact.merge(payments, on="order_id", how="left")
    fact = fact.merge(reviews,  on="order_id", how="left")
    fact = fact.merge(dim_customers[["customer_key", "customer_id"]], on="customer_id", how="left")
    fact = fact.merge(dim_products[["product_key",  "product_id"]],  on="product_id",  how="left")
    fact = fact.merge(dim_sellers[["seller_key",   "seller_id"]],   on="seller_id",   how="left")

    fact["date_key"] = pd.to_datetime(fact["order_purchase_timestamp"]).dt.strftime("%Y%m%d").astype(int)

    fact = fact[["order_id", "order_item_id", "customer_key", "product_key",
                 "seller_key", "date_key", "price", "freight_value",
                 "payment_value", "review_score"]]
    fact = fact.dropna(subset=["customer_key", "product_key", "seller_key"])
    for col in ["customer_key", "product_key", "seller_key"]:
        fact[col] = fact[col].astype(int)
    return fact


# ── Load ──────────────────────────────────────────────────────────────────────

def load(df, table_name):
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"  ✓ {table_name}: {len(df):,} rows")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=== Extract ===")
    customers, orders, order_items, order_payments, order_reviews, products, sellers, category_trans = extract()

    print("=== Transform ===")
    dim_customers = build_dim_customers(customers)
    dim_products  = build_dim_products(products, category_trans)
    dim_sellers   = build_dim_sellers(sellers)
    dim_date      = build_dim_date(orders)
    fact          = build_fact_order_items(
        order_items, orders, order_payments, order_reviews,
        dim_customers, dim_products, dim_sellers
    )

    print("=== Load ===")
    load(dim_customers, "dim_customers")
    load(dim_products,  "dim_products")
    load(dim_sellers,   "dim_sellers")
    load(dim_date,      "dim_date")
    load(fact,          "fact_order_items")

    print("\nDone! Star schema loaded into olist_dw.")


if __name__ == "__main__":
    main()

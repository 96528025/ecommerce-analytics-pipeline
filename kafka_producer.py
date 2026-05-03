"""
Kafka Producer: simulates real-time order stream from Olist dataset.
Reads orders + items CSV and sends each order as a JSON message.
"""
import json
import time
import pandas as pd
from kafka import KafkaProducer

TOPIC = "olist-orders"
DELAY = 0.05  # seconds between messages (20 orders/sec)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

orders      = pd.read_csv("olist_orders_dataset.csv")
order_items = pd.read_csv("olist_order_items_dataset.csv")

# Aggregate total price per order
totals = order_items.groupby("order_id").agg(
    item_count=("order_item_id", "count"),
    total_price=("price", "sum"),
    total_freight=("freight_value", "sum"),
).reset_index()

df = orders.merge(totals, on="order_id", how="inner")
df = df[["order_id", "customer_id", "order_status",
         "order_purchase_timestamp", "item_count",
         "total_price", "total_freight"]].dropna()

print(f"Sending {len(df):,} orders to topic '{TOPIC}'...")
print("Press Ctrl+C to stop.\n")

for i, row in df.iterrows():
    msg = row.to_dict()
    producer.send(TOPIC, value=msg)

    if (i + 1) % 500 == 0:
        print(f"  Sent {i + 1:,} / {len(df):,} orders")

    time.sleep(DELAY)

producer.flush()
print("\nAll orders sent.")

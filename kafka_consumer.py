"""
Kafka Consumer: processes real-time order stream.
Tracks running GMV, order count, and flags high-value orders.
"""
import json
from kafka import KafkaConsumer

TOPIC = "olist-orders"
HIGH_VALUE_THRESHOLD = 500.0

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=10000,  # stop after 10s of no messages
)

print(f"Listening to topic '{TOPIC}'...")
print(f"High-value order threshold: ${HIGH_VALUE_THRESHOLD}\n")

total_orders = 0
total_gmv    = 0.0
high_value   = 0

for msg in consumer:
    order = msg.value
    price = float(order.get("total_price", 0))

    total_orders += 1
    total_gmv    += price

    if price >= HIGH_VALUE_THRESHOLD:
        high_value += 1
        print(f"  ⭐ High-value order: {order['order_id'][:8]}... "
              f"${price:.2f} ({order['item_count']} items)")

    if total_orders % 1000 == 0:
        print(f"  [{total_orders:,} orders] "
              f"GMV: ${total_gmv:,.2f} | "
              f"Avg: ${total_gmv/total_orders:.2f} | "
              f"High-value: {high_value}")

print(f"\n=== Final Summary ===")
print(f"Total orders processed : {total_orders:,}")
print(f"Total GMV              : ${total_gmv:,.2f}")
print(f"Average order value    : ${total_gmv/total_orders:.2f}")
print(f"High-value orders      : {high_value} ({high_value/total_orders*100:.1f}%)")

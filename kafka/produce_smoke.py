# kafka/produce_smoke.py
import os, json, time, random
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_TRADES_RAW", "trades.raw")

p = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

def dr(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Produced to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

# 5 good messages
now = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
good = [
    {
        "type": "trade",
        "product_id": "BTC-USD",
        "time": now + "Z",
        "price": f"{67600 + i * 10:.2f}",
        "size": f"{0.0010 + i * 0.0001:.4f}",
        "side": "buy" if i % 2 == 0 else "sell",
        "trade_id": 100000 + i
    }
    for i in range(5)
]

for g in good:
    p.produce(TOPIC, json.dumps(g).encode("utf-8"), on_delivery=dr)
    p.poll(0)

# 1 malformed (invalid JSON)
bad = '{"type":"trade","product_id": BTC-USD,"time":"' + now + 'Z","price":"999.99"}'
p.produce(TOPIC, bad.encode("utf-8"), on_delivery=dr)
p.flush(10)
print("Smoke messages sent.")

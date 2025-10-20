"""
The start of the ingestion pipeline 
Pulls from Coinbase Advanced Trade's market_trades channel.
"""



import argparse, asyncio, json, os, random, ssl, time
from typing import Any, Dict, List, Tuple

import websockets
from dotenv import load_dotenv

from .common import (
    SCHEMA_VERSION, jlog, build_producer, publish_json, flush_quiet, load_pairs,
    utc_now_iso, safe_float, validate_trade, send_to_dlq, GracefulExit
)


load_dotenv()

RAW_TOPIC = os.getenv("KAFKA_TOPIC_TRADES_RAW", "trades.raw")
DLQ_TOPIC = os.getenv("KAFKA_TOPIC_TRADES_DLQ", "trades.raw.dlq")



#Normalization 
"""
Takes in the data from market_trades and normal's it in a schema with these tags
schema_version
source: 
type: 
product_id:
trade_id
price:
size:
side:
ts_event:
ts_ingest:
"""
def normalize_market_trades(msg: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if msg.get("type") != "market_trades":
        return out
    product = msg.get("product_id")
    for t in msg.get("trades") or []:
        rec = {
            "schema_version": SCHEMA_VERSION,
            "source": "coinbase",
            "type": "trade",
            "product_id": product,
            "trade_id": t.get("trade_id"),
            "price": safe_float(t.get("price")),
            "size": safe_float(t.get("size")),
            "side": (str(t.get("side")).lower() if t.get("side") else None),
            "ts_event": t.get("time") or utc_now_iso(),
            "ts_ingest": utc_now_iso(),
        }
        out.append(rec)
    return out


#For testing pulls from market_trades channal "mock"
def gen_mock_trade(product_id: str) -> Dict[str, Any]:
    price = float(f"{random.uniform(20000,80000):.2f}") if product_id.startswith("BTC") \
            else float(f"{random.uniform(900,6000):.2f}")
    size  = float(f"{random.uniform(0.0004,0.6):.6f}")
    side  = random.choice(["buy","sell"])
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "mock",
        "type": "trade",
        "product_id": product_id,
        "trade_id": random.randint(10_000_000, 99_999_999),
        "price": price,
        "size": size,
        "side": side,
        "ts_event": utc_now_iso(),
        "ts_ingest": utc_now_iso(),
    }
async def run_mock(topic: str, products: List[str], rate: float, burst: int, stop: GracefulExit):
    p = build_producer()
    delay = max(0.01, 1.0/max(rate, 1e-6))
    jlog("info","mock_started", topic=topic, products=products, rate=rate, burst=burst)
    try:
        while not stop.should_stop:
            for _ in range(burst):
                rec = gen_mock_trade(random.choice(products))
                ok, reason = validate_trade(rec)
                if ok:
                    publish_json(p, topic, rec, key=rec["product_id"])
                else:
                    send_to_dlq(p, DLQ_TOPIC, reason or "invalid", rec)
            await asyncio.sleep(delay)
    finally:
        flush_quiet(p)
        jlog("info","mock_stopped")


#Coinbase with Backoff
async def run_coinbase(topic: str, url: str, products: List[str], stop: GracefulExit):
    p = build_producer()
    backoff_base = int(os.getenv("WS_BACKOFF_BASE_MS","250"))
    backoff_max  = int(os.getenv("WS_BACKOFF_MAX_MS","10000"))
    insecure_ssl = os.getenv("COINBASE_INSECURE_SSL","false").lower() in {"1","true","yes"}

    ssl_ctx = ssl.create_default_context()
    if insecure_ssl:
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        jlog("warn","insecure_ssl_enabled")

    subscribe_payload = {"type":"subscribe","channel":"market_trades","product_ids":products}
    attempt = 0

    while not stop.should_stop:
        try:
            jlog("info","ws_connecting", url=url, products=products)
            async with websockets.connect(url, ssl=ssl_ctx, ping_interval=20, ping_timeout=20) as ws:
                jlog("info","ws_open")
                await ws.send(json.dumps(subscribe_payload))
                attempt = 0  # reset backoff on success

                async for raw in ws:
                    if stop.should_stop:
                        break
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    t = msg.get("type")
                    if t in {"subscriptions","heartbeat","status"}:
                        continue

                    for rec in normalize_market_trades(msg):
                        ok, reason = validate_trade(rec)
                        if ok:
                            publish_json(p, topic, rec, key=rec["product_id"])
                        else:
                            send_to_dlq(p, DLQ_TOPIC, reason or "invalid", rec)

        except asyncio.CancelledError:
            break
        except Exception as e:
            # Backoff with jitter
            attempt += 1
            sleep_ms = min(backoff_max, backoff_base * (2 ** (attempt - 1)))
            sleep_ms = int(sleep_ms * random.uniform(0.7, 1.3))
            jlog("warn","ws_error_retrying", attempt=attempt, error=str(e), sleep_ms=sleep_ms)
            await asyncio.sleep(sleep_ms/1000.0)

    flush_quiet(p)
    jlog("info","ws_stopped")

#Main
def main():
    parser = argparse.ArgumentParser(description="Coinbase Advanced Trade WS → Kafka (production-grade)")
    parser.add_argument("--topic", default=RAW_TOPIC)
    parser.add_argument("--url", default=os.getenv("COINBASE_WS_URL","wss://advanced-trade-ws.coinbase.com"))
    parser.add_argument("--pairs-file", default=os.getenv("PAIRS_FILE","configs/pairs.yml"))
    parser.add_argument("--mock", action="store_true", help="Run local mock generator")
    parser.add_argument("--rate", type=float, default=15.0, help="[MOCK] msgs/sec total")
    parser.add_argument("--burst", type=int, default=1, help="[MOCK] messages per tick")
    args = parser.parse_args()

    products = load_pairs(args.pairs_file)
    if not products:
        raise SystemExit(f"No products in {args.pairs_file}")

    jlog("info","ingestion_start", mock=args.mock, topic=args.topic, url=args.url, products=products)
    stop = GracefulExit()
    if args.mock:
        asyncio.run(run_mock(args.topic, products, args.rate, args.burst, stop))
    else:
        asyncio.run(run_coinbase(args.topic, args.url, products, stop))
    jlog("info","ingestion_exit")

if __name__ == "__main__":
    main()
"""
Centralized all utility files into 1 area, every ingestion script imports the same "blocks"
Blocks:
time helpers
structured logging of coins
JSON encode/decode 
schema/version
validation
kafka prducer constrution
DLQ emission
config loading and shutdown signal
Keeps logic within coinbase_ws.py and binance.py (future) normalized with each producer
producers:
future exchanges
REST ingestor 
replay tools
Basically creates identical behavior accross each coin
"""

"""
IMPORTANT: MOST OF THIS IS JUST A HELPER SCRIPT AND PRETTY STANDARD NOT REALLY IMPORTANT TO UNDERSTAND
JUST IMPORTANT SO WE DON"T HAVE TO DO THIS 10 times in a row with every new import
"""

import json, os, time, signal, sys, random
from typing import Any, Dict, Optional, List, Tuple
from datetime import datetime, timezone

import yaml
from dotenv import load_dotenv
from confluent_kafka import Producer 

load_dotenv()

#hardcoded schema version for now
SCHEMA_VERSION = "1.0.0"


#Structured logging (self-conatined data record of each object pulled)

def jlog(level: str, msg: str, **fields):
    rec = {"ts": utc_now_iso(), "level": level, "msg": msg, **fields}
    print(json.dumps(rec, separators=(",", ":"), ensure_ascii=False), flush=True)

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


#JSON BYTES
def to_json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

#Casting and Validation 
def safe_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None

def validate_trade(rec: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Minimal but production-oriented validator for RAW records.
    Required: source, type='trade', product_id, price, size, side, ts_event, ts_ingest
    """
    required = ["source","type","product_id","price","size","side","ts_event","ts_ingest"]
    for k in required:
        if rec.get(k) in (None, ""):
            return False, f"missing_field:{k}"
    if rec.get("type") != "trade":
        return False, "bad_type"
    if rec.get("side") not in ("buy","sell"):
        return False, "bad_side"
    try:
        float(rec["price"]); float(rec["size"])
    except Exception:
        return False, "bad_numeric"
    return True, None


#Kafka report + production report
def _delivery_report(err, msg) -> None:
    if err is not None:
        jlog("warn","delivery_failed", error=str(err), topic=msg.topic())
    else:
        # Keep it lightweight; offsets are handy during bring-up
        jlog("debug","delivery_ok", topic=msg.topic(), partition=msg.partition(), offset=msg.offset())

def build_producer() -> Producer:
    conf = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS","localhost:9092"),
        "enable.idempotence": True,
        "linger.ms": 5,
        "acks": "all",
        "retries": 5,
        "compression.type": "zstd",
        "queue.buffering.max.messages": 100000,  # default is generous; okay for local dev
    }
    return Producer(conf)

def _produce_with_backpressure(p: Producer, topic: str, key: Optional[str], value_bytes: bytes):
    while True:
        try:
            p.produce(topic=topic, key=key.encode("utf-8") if key else None, value=value_bytes, on_delivery=_delivery_report)
            break
        except BufferError:
            # Local queue full; give librdkafka a moment to deliver
            p.poll(0.1)

def publish_json(p: Producer, topic: str, value: Dict[str, Any], key: Optional[str] = None) -> None:
    _produce_with_backpressure(p, topic, key, to_json_bytes(value))
    p.poll(0)

def flush_quiet(p: Producer, timeout: float = 10.0) -> None:
    t0 = time.time()
    p.flush(timeout)
    jlog("info","producer_flush_done", elapsed=f"{time.time()-t0:.3f}s")


#DLQ
def send_to_dlq(p: Producer, dlq_topic: str, reason: str, original: Dict[str, Any]):
    payload = {
        "schema_version": SCHEMA_VERSION,
        "reason": reason,
        "original": original,
        "ts_ingest": utc_now_iso(),
    }
    publish_json(p, dlq_topic, payload, key=original.get("product_id"))

#Config 
def load_pairs(path: str) -> List[str]:
    with open(path, "r") as f:
        doc = yaml.safe_load(f) or {}
    prods = doc.get("products") or []
    if not isinstance(prods, list):
        raise ValueError("configs/pairs.yml must have a list under key 'products'")
    return [str(x).strip() for x in prods if str(x).strip()]


#Signals 
class GracefulExit:
    def __init__(self):
        self._stop = False
        signal.signal(signal.SIGINT, self._handler)
        signal.signal(signal.SIGTERM, self._handler)
    def _handler(self, *_):
        self._stop = True
    @property
    def should_stop(self) -> bool:
        return self._stop

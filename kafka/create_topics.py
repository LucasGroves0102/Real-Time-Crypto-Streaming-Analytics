#!/usr/bin/env python3
import argparse
import sys
from typing import Dict, Any, List

try:
    import yaml
except ImportError:
    print("Missing dependency: pyyaml. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(2)

try:
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    print("Missing dependency: confluent-kafka. Install with: pip install confluent-kafka", file=sys.stderr)
    sys.exit(2)


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if "topics" not in data or not isinstance(data["topics"], list):
        raise ValueError("YAML must contain a top-level 'topics: [...]' list.")
    return data


def normalize_topic_spec(t: Dict[str, Any]) -> Dict[str, Any]:
    # Basic validation + defaults (be explicit to help debugging)
    name = str(t.get("name", "")).strip()
    if not name:
        raise ValueError("Topic entry missing 'name'.")
    partitions = int(t.get("partitions", 1))
    replication_factor = int(t.get("replication_factor", 1))
    config = t.get("config") or {}
    if not isinstance(config, dict):
        raise ValueError(f"Topic '{name}' has non-dict 'config'.")
    # All config values must be strings for the Admin API
    config_str = {str(k): str(v) for k, v in config.items()}
    return {
        "name": name,
        "partitions": partitions,
        "replication_factor": replication_factor,
        "config": config_str,
    }


def create_missing_topics(bootstrap: str, topics: List[Dict[str, Any]], dry_run: bool = False) -> int:
    admin = AdminClient({"bootstrap.servers": bootstrap})

    # Get existing topics (names only)
    md = admin.list_topics(timeout=10)
    existing = set(md.topics.keys())

    to_create: List[NewTopic] = []
    plan: List[str] = []

    for spec in topics:
        s = normalize_topic_spec(spec)
        if s["name"] in existing:
            plan.append(f"= exists: {s['name']}")
            continue
        to_create.append(
            NewTopic(
                topic=s["name"],
                num_partitions=s["partitions"],
                replication_factor=s["replication_factor"],
                config=s["config"] if s["config"] else None,
            )
        )
        plan.append(f"+ create: {s['name']} [p={s['partitions']}, rf={s['replication_factor']}, cfg={s['config']}]")

    print("Plan:")
    for line in plan:
        print("  " + line)

    if dry_run or not to_create:
        print("Nothing to do." if not to_create else "Dry-run: not creating.")
        return 0

    # Create and wait for results
    fs = admin.create_topics(to_create, request_timeout=30)
    errors = 0
    for name, f in fs.items():
        try:
            f.result()
            print(f"✓ created: {name}")
        except Exception as e:
            errors += 1
            print(f"✗ failed:  {name} -> {e}", file=sys.stderr)
    return errors


def main():
    ap = argparse.ArgumentParser(description="Create missing Kafka topics from YAML.")
    ap.add_argument("--config", required=True, help="Path to configs/kafka-topics.yml")
    ap.add_argument("--bootstrap", default="localhost:9092", help="Bootstrap servers (host:port)")
    ap.add_argument("--dry-run", action="store_true", help="Show what would happen without creating")
    args = ap.parse_args()

    data = load_config(args.config)
    topics = data["topics"]
    rc = create_missing_topics(args.bootstrap, topics, dry_run=args.dry_run)
    sys.exit(rc)


if __name__ == "__main__":
    main()

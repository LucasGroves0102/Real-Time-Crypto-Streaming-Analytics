# Loads variables from .env and exports to all recipes
include .env
export

# Defaults if .env is missing
KAFKA_BROKER        ?= localhost:9092
TOPIC_SMOKE         ?= smoke.test
KAFKA_TOPIC_TRADES_RAW ?= trades.raw
KAFKA_TOPIC_TRADES_DLQ ?= trades.raw.dlq
PAIRS_FILE          ?= configs/pairs.yml

DOCKER := docker-compose
BROKER := $(KAFKA_BROKER)
SMOKE_TOPIC := $(TOPIC_SMOKE)

.PHONY: help up down logs create-topic list-topics smoke clean \
        create-topics dry-run-topics create-core-topics \
        produce-mock produce-coinbase tail-raw tail-dlq

help:
	@echo "Targets:"
	@echo "  up                  - Start Zookeeper/Kafka"
	@echo "  down                - Stop and remove containers/volumes"
	@echo "  logs                - Tail Kafka container logs"
	@echo "  create-topic        - Create $(SMOKE_TOPIC) (smoke)"
	@echo "  list-topics         - List topics via docker exec"
	@echo "  smoke               - Produce/consume a test message"
	@echo "  create-topics       - Create topics from configs/kafka-topics.yml"
	@echo "  dry-run-topics      - Dry run topic creation from YAML"
	@echo "  create-core-topics  - Imperative create trades.raw + trades.raw.dlq"
	@echo "  produce-mock        - Run mock producer → $(KAFKA_TOPIC_TRADES_RAW)"
	@echo "  produce-coinbase    - Run Coinbase WS producer"
	@echo "  tail-raw            - Consume from $(KAFKA_TOPIC_TRADES_RAW) (docker exec)"
	@echo "  tail-dlq            - Consume from $(KAFKA_TOPIC_TRADES_DLQ) (docker exec)"
	@echo "  clean               - Remove ./data and ./checkpoints contents (safe)"

# Starts Zookeeper and Kafka
up:
	$(DOCKER) up -d
	@echo "Waiting for Kafka to be ready"
	@sleep 5
	@$(DOCKER) ps

# Stop and remove containers, networks, volumes
down:
	$(DOCKER) down -v

# Follow Kafka logs
logs:
	$(DOCKER) logs -f kafka

# Create the single smoke topic
create-topic:
	@echo "Creating topic: $(SMOKE_TOPIC)"
	@docker exec kafka kafka-topics \
		--bootstrap-server kafka:9093 \
		--create --if-not-exists \
		--topic "$(SMOKE_TOPIC)" \
		--partitions 1 --replication-factor 1
	@$(MAKE) list-topics

# List topics inside the broker
list-topics:
	@docker exec kafka kafka-topics --bootstrap-server kafka:9093 --list

# Sanity check (Produce 3 messages, consume 3 messages)
smoke:
	@bash kafka/smoke_test.sh

# Create topics from YAML (declarative)
create-topics:
	@python3 kafka/create_topics.py --config configs/kafka-topics.yml --bootstrap "$(BROKER)"

dry-run-topics:
	@python3 kafka/create_topics.py --config configs/kafka-topics.yml --bootstrap "$(BROKER)" --dry-run

# Quick imperative creation of core ingestion topics (optional convenience)
create-core-topics:
	@echo "Ensuring $(KAFKA_TOPIC_TRADES_RAW) exists"
	@docker exec kafka kafka-topics --bootstrap-server kafka:9093 --create --if-not-exists \
		--topic "$(KAFKA_TOPIC_TRADES_RAW)" --partitions 3 --replication-factor 1
	@echo "Ensuring $(KAFKA_TOPIC_TRADES_DLQ) exists"
	@docker exec kafka kafka-topics --bootstrap-server kafka:9093 --create --if-not-exists \
		--topic "$(KAFKA_TOPIC_TRADES_DLQ)" --partitions 1 --replication-factor 1 \
		--config retention.ms=1209600000
	@$(MAKE) list-topics

# Ingestion producers (host Python; reads .env)
produce-mock:
	@python -m ingestion.coinbase_ws --mock --pairs-file "$(PAIRS_FILE)" --rate 20

produce-coinbase:
	@python -m ingestion.coinbase_ws --pairs-file "$(PAIRS_FILE)"

# Tail topics from inside the Kafka container (no local CLI required)
tail-raw:
	@docker exec -it kafka kafka-console-consumer \
		--bootstrap-server kafka:9093 \
		--topic "$(KAFKA_TOPIC_TRADES_RAW)" --from-beginning

tail-dlq:
	@docker exec -it kafka kafka-console-consumer \
		--bootstrap-server kafka:9093 \
		--topic "$(KAFKA_TOPIC_TRADES_DLQ)" --from-beginning

# Local cleanup (lakehouse dirs) -- SAFE
clean:
	@echo "Cleaning ./data and ./checkpoints contents"
	@mkdir -p data checkpoints
	@rm -rf data/* checkpoints/* 2>/dev/null || true

.PHONY: bronze-run bronze-backfill bronze-smoke bronze-clean

# Run the Bronze stream (Kafka → Parquet)
bronze-run:
	@echo "Starting Bronze stream..."
	spark-submit \
	  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
	  spark/bronze_stream.py

# Backfill from earliest offsets (optional)
bronze-backfill:
	@echo "Starting Bronze stream from earliest offsets..."
	STARTING_OFFSETS=earliest spark-submit \
	  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
	  spark/bronze_stream.py

# Send test messages to trades.raw to validate end-to-end Bronze
bronze-smoke:
	@echo "Producing 5 good + 1 malformed test messages to $(KAFKA_TOPIC_TRADES_RAW)..."
	python kafka/produce_smoke.py
	@echo "Now check data/bronze/trades/ and data/bronze/trades_dlq/ for Parquet files."

# Remove Bronze data and checkpoints (CAUTION)
bronze-clean:
	@echo "WARNING: deleting Bronze Parquet data and checkpoints..."
	rm -rf data/bronze/trades data/bronze/trades_dlq checkpoints/bronze/trades checkpoints/bronze/trades_dlq
	@echo "Done."
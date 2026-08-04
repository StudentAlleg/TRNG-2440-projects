"""Part E -- Kafka producer and consumer.

Both run on your machine in either mode: Databricks Free Edition cannot reach a
broker on localhost, so events are always published from the local process
after reading the Gold table.

    python -m retailpulse.kafka_io publish
    python -m retailpulse.kafka_io consume --max 20
"""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Iterable
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer

from .config import RunConfig, load_config
from .events import build_events

log = logging.getLogger("kafka_io")

EVENT_COLUMNS = ["order_month", "category", "order_count", "total_quantity", "total_revenue"]


def publish_events(cfg: RunConfig, events: Iterable[dict[str, Any]]) -> int:
    """Publish events keyed by `event_id`.  Returns the delivered count."""
    producer = Producer({"bootstrap.servers": cfg.kafka_bootstrap})
    delivered = 0
    failed: list[str] = []

    def on_delivery(err, msg):
        nonlocal delivered
        if err is not None:
            failed.append(str(err))
            log.error("delivery failed: %s", err)
        else:
            delivered += 1
            log.info(
                "delivered %s -> %s[%d]@%d",
                msg.key().decode(), msg.topic(), msg.partition(), msg.offset(),
            )

    for event in events:
        producer.produce(
            topic=cfg.kafka_topic,
            key=event["event_id"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            on_delivery=on_delivery,
        )
        producer.poll(0)

    producer.flush(30)
    if failed:
        raise RuntimeError(f"{len(failed)} event(s) failed to publish: {failed[0]}")
    return delivered


def consume_events(cfg: RunConfig, max_messages: int | None = None, timeout: float = 10.0) -> int:
    """Print every message and append it to `cfg.consumed_events_path`.

    Stops after `max_messages` messages, or after `timeout` seconds with an
    empty poll so it terminates cleanly inside an Airflow task.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": cfg.kafka_bootstrap,
            "group.id": cfg.kafka_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([cfg.kafka_topic])
    cfg.consumed_events_path.parent.mkdir(parents=True, exist_ok=True)

    consumed = 0
    try:
        with cfg.consumed_events_path.open("a", encoding="utf-8") as sink:
            while max_messages is None or consumed < max_messages:
                msg = consumer.poll(timeout)
                if msg is None:
                    break
                err = msg.error()
                if err is not None:
                    if err.code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(err)
                payload = msg.value().decode("utf-8")
                print(payload)
                sink.write(payload + "\n")
                consumed += 1
    finally:
        consumer.close()
    return consumed


def read_gold_events(cfg: RunConfig) -> list[dict[str, Any]]:
    """Gold monthly-category rows -> event payloads.  Needs a Spark session."""
    from .session import get_spark

    rows = (
        get_spark(cfg)
        .table(cfg.table("gold_monthly_category_sales"))
        .select(*EVENT_COLUMNS)
        .orderBy("order_month", "category")
        .collect()
    )
    return build_events([r.asDict() for r in rows])


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description=(__doc__ or "").strip().splitlines()[0])
    parser.add_argument("action", choices=["publish", "consume"])
    parser.add_argument("--mode", choices=["local", "databricks"], default=None)
    parser.add_argument("--max", type=int, default=None, help="consume: stop after N messages")
    args = parser.parse_args()

    cfg = load_config(args.mode)
    if args.action == "publish":
        events = read_gold_events(cfg)
        delivered = publish_events(cfg, events)
        log.info("published %d/%d events to %s", delivered, len(events), cfg.kafka_topic)
    else:
        consumed = consume_events(cfg, args.max)
        log.info("consumed %d events -> %s", consumed, cfg.consumed_events_path)


if __name__ == "__main__":
    main()

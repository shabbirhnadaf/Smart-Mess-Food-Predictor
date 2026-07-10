"""
Simulates college event feed → Kafka topic: event-feed
Run: python kafka/producer/event_producer.py
"""
import json, random, time, os, sys, uuid
from datetime import datetime, timezone, timedelta
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv
from loguru import logger

load_dotenv()
BS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_EVENTS", "event-feed")

EVENT_TYPES   = ["exam","holiday","sports_day","cultural_fest",
                 "guest_lecture","industry_visit","normal"]
IMPACT_SCORES = {"exam": 0.3, "holiday": 1.4, "sports_day": 1.2,
                 "cultural_fest": 1.5, "guest_lecture": 0.9,
                 "industry_visit": 0.6, "normal": 1.0}


def make_event():
    etype = random.choice(EVENT_TYPES)
    return {
        "event_id":      str(uuid.uuid4()),
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "event_type":    etype,
        "event_name":    f"{etype.replace('_',' ').title()} {random.randint(1,5)}",
        "impact_score":  IMPACT_SCORES[etype],
        "affected_blocks": random.sample(
            ["A","B","C","D","E","F","Girls_Block"],
            k=random.randint(1, 7)
        ),
        "is_holiday":    1 if etype == "holiday" else 0,
        "is_exam_week":  1 if etype == "exam" else 0,
        "duration_days": random.randint(1, 3),
        "expected_attendance_pct": round(random.uniform(0.4, 1.0), 2),
        "source":        "academic_calendar"
    }


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")


def connect():
    try:
        admin = AdminClient({
            "bootstrap.servers":    BS,
            "broker.address.family": "v4"
        })
        admin.list_topics(timeout=8)
        logger.info(f"Broker reachable at {BS}")
    except Exception as e:
        logger.error(f"Cannot reach broker: {e}")
        sys.exit(1)
    p = Producer({
        "bootstrap.servers":     BS,
        "broker.address.family": "v4",
        "acks":                  "all",
        "compression.type":      "gzip",
    })
    logger.info(f"Event producer connected → {BS}")
    return p


def run():
    prod = connect()
    sent = 0
    logger.info(f"Producing college events every 60s → [{TOPIC}]")
    try:
        while True:
            ev = make_event()
            prod.produce(TOPIC, value=json.dumps(ev).encode(), callback=delivery_report)
            prod.flush()
            sent += 1
            logger.info(f"Event sent #{sent}: {ev['event_name']} "
                        f"(impact={ev['impact_score']}, exam={ev['is_exam_week']})")
            time.sleep(60)
    except KeyboardInterrupt:
        prod.flush()
        logger.info(f"Event producer stopped. Total: {sent}")


if __name__ == "__main__":
    run()
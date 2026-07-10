"""
Simulates RFID swipe events → Kafka topic: mess-orders
Run: python kafka/producer/order_producer.py --rate 5 --students 500
"""
import json, random, time, uuid, os, sys, argparse
from datetime import datetime, timezone
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv
from loguru import logger

load_dotenv()
BS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_ORDERS", "mess-orders")

FOODS  = ["idli","dosa","poha","upma","bread_butter","rice","chapati","dal",
          "sambar","rasam","veg_curry","egg_curry","curd_rice","pulao","biryani"]
BLOCKS = ["A","B","C","D","E","F","Girls_Block"]


def get_slot():
    h = datetime.now().hour
    if 7 <= h < 10:  return "breakfast"
    if 12 <= h < 15: return "lunch"
    if 19 <= h < 22: return "dinner"
    return random.choice(["breakfast","lunch","dinner"])


def make_event(sid):
    items = random.sample(FOODS, k=random.randint(2, 5))
    return {
        "event_id":    str(uuid.uuid4()),
        "student_id":  sid,
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "meal_slot":   get_slot(),
        "food_items":  items,
        "quantities":  {i: random.randint(1, 3) for i in items},
        "hostel_block": random.choice(BLOCKS),
        "day_type":    "weekend" if datetime.now().weekday() >= 5 else "weekday",
        "swipe_count": random.randint(1, 3),
        "source":      "rfid_terminal"
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
        meta = admin.list_topics(timeout=8)
        logger.info(f"Broker reachable. Topics: {list(meta.topics.keys())}")
    except Exception as e:
        logger.error(f"Cannot reach broker at {BS}: {e}")
        sys.exit(1)

    p = Producer({
        "bootstrap.servers":     BS,
        "broker.address.family": "v4",
        "acks":                  "all",
        "compression.type":      "gzip",
        "linger.ms":             10,
        "batch.size":            16384,
    })
    logger.info(f"Producer connected → {BS}")
    return p

def run(rate, total_students):
    prod  = connect()
    sids  = [f"STU{str(i).zfill(5)}" for i in range(1, total_students + 1)]
    inv   = 1.0 / max(rate, 1)
    sent  = 0
    logger.info(f"Producing {rate} ev/s, {total_students} students → [{TOPIC}]")
    try:
        while True:
            ev = make_event(random.choice(sids))
            prod.produce(
                TOPIC,
                key=ev["student_id"],
                value=json.dumps(ev).encode(),
                callback=delivery_report
            )
            prod.poll(0)
            sent += 1
            if sent % 200 == 0:
                prod.flush()
                logger.info(f"Sent {sent:,} events to [{TOPIC}]")
            time.sleep(inv)
    except KeyboardInterrupt:
        prod.flush()
        logger.info(f"Stopped. Total sent: {sent:,}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--rate",     type=int, default=5)
    p.add_argument("--students", type=int, default=500)
    a = p.parse_args()
    run(a.rate, a.students)
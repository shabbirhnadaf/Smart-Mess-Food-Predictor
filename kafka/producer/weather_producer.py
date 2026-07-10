"""
Simulates weather feed → Kafka topic: weather-feed
Run: python kafka/producer/weather_producer.py
"""
import json, random, time, os, sys
from datetime import datetime, timezone
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv
from loguru import logger

load_dotenv()
BS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_WEATHER", "weather-feed")

CONDITIONS = ["sunny","cloudy","rainy","foggy","windy","humid"]


def make_weather():
    return {
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(18.0, 38.0), 1),
        "humidity":    random.randint(40, 95),
        "rainfall_mm": round(random.uniform(0.0, 25.0), 2)
                       if random.random() < 0.2 else 0.0,
        "condition":   random.choice(conditions := Conditions),
        "weather_score": round(random.uniform(0.1, 1.0), 2),
        "source":      "weather_station"
    } 


def make_weather():
    cond = random.choice(Conditions := CONDITIONS)
    rain = round(random.uniform(0.0, 25.0), 2) if random.random() < 0.2 else 0.0
    return {
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "temperature":   round(random.uniform(18.0, 38.0), 1),
        "humidity":      random.randint(40, 95),
        "rainfall_mm":   rain,
        "condition":     cond,
        "weather_score": round(random.uniform(0.1, 1.0), 2),
        "source":        "weather_station"
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
    logger.info(f"Weather producer connected → {BS}")
    return p


def run():
    prod = connect()
    sent = 0
    logger.info(f"Producing weather events every 30s → [{TOPIC}]")
    try:
        while True:
            ev = make_weather()
            prod.produce(TOPIC, value=json.dumps(ev).encode(), callback=delivery_report)
            prod.flush()
            sent += 1
            logger.info(f"Weather event sent #{sent}: temp={ev['temperature']}°C, "
                        f"cond={ev['condition']}, rain={ev['rainfall_mm']}mm")
            time.sleep(30)
    except KeyboardInterrupt:
        prod.flush()
        logger.info(f"Weather producer stopped. Total: {sent}")


if __name__ == "__main__":
    run()
"""Simulated FinTech payment gateway: publishes JSON transactions to Kafka.
~4% are fraud-like, ~5% are deliberately malformed to exercise the data-quality gates."""
import json, os, random, time, uuid
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "transactions")
RATE = float(os.getenv("EVENTS_PER_SEC", "15"))

CATEGORIES = ["grocery", "fuel", "travel", "electronics", "dining", "crypto", "gambling", "utilities"]
COUNTRIES = ["US", "GB", "DE", "IN", "SG", "FR"]
CURRENCIES = ["USD", "EUR", "GBP", "INR", "JPY"]
CHANNELS = ["pos", "online", "mobile", "atm"]
MERCHANTS = [f"M{i:04d}" for i in range(1, 61)]


def now_str(delta=timedelta(0)):
    return (datetime.now(timezone.utc) + delta).strftime("%Y-%m-%d %H:%M:%S")


def good_event():
    home = random.choice(COUNTRIES)
    evt = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"U{random.randint(1, 800):05d}",
        "card_id": f"C{random.randint(1, 1200):06d}",
        "merchant_id": random.choice(MERCHANTS),
        "merchant_category": random.choice(CATEGORIES),
        "amount": round(random.lognormvariate(3.6, 1.0), 2),
        "currency": random.choice(CURRENCIES),
        "card_country": home,
        "merchant_country": home,
        "channel": random.choice(CHANNELS),
        "event_time": now_str(),
        "schema_version": 1,
    }
    if random.random() < 0.04:  # fraud-like pattern
        evt["amount"] = round(random.uniform(5000, 9500), 2)
        evt["merchant_country"] = random.choice([c for c in COUNTRIES if c != home])
        evt["channel"] = "online"
        evt["merchant_category"] = random.choice(["crypto", "gambling", "electronics"])
    return evt


def corrupt(evt):
    kind = random.choice(["no_id", "neg_amount", "bad_currency", "garbage", "future", "schema_v3"])
    if kind == "garbage":
        return b"{this is not json"
    if kind == "no_id":
        evt.pop("transaction_id")
    elif kind == "neg_amount":
        evt["amount"] = -abs(evt["amount"])
    elif kind == "bad_currency":
        evt["currency"] = "XXX"
    elif kind == "future":
        evt["event_time"] = now_str(timedelta(days=2))
    elif kind == "schema_v3":  # simulated upstream schema drift
        evt["schema_version"] = 3
        evt["new_field"] = "surprise"
    return json.dumps(evt).encode()


def connect():
    while True:
        try:
            return KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all", linger_ms=20)
        except NoBrokersAvailable:
            print("Kafka not ready, retrying...", flush=True)
            time.sleep(3)


if __name__ == "__main__":
    producer = connect()
    print(f"Producing ~{RATE}/s to {TOPIC} @ {BOOTSTRAP}", flush=True)
    sent = 0
    while True:
        evt = good_event()
        payload = corrupt(evt) if random.random() < 0.05 else json.dumps(evt).encode()
        producer.send(TOPIC, key=evt["user_id"].encode(), value=payload)
        sent += 1
        if sent % 200 == 0:
            print(f"sent {sent}", flush=True)
        time.sleep(1.0 / RATE)

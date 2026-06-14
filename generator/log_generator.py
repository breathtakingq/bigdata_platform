import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


SERVICES = [
    "auth-service",
    "payment-service",
    "order-service",
    "catalog-service",
    "notification-service",
]

ENDPOINTS = {
    "auth-service": ["/login", "/logout", "/refresh-token", "/profile"],
    "payment-service": ["/pay", "/refund", "/invoice", "/card/validate"],
    "order-service": ["/orders", "/orders/create", "/orders/cancel", "/orders/status"],
    "catalog-service": ["/products", "/products/search", "/products/recommendations"],
    "notification-service": ["/email/send", "/sms/send", "/push/send"],
}

ERROR_CODES = [400, 401, 403, 404, 409, 429, 500, 502, 503]
HTTP_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def wait_for_kafka(bootstrap_servers: str) -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda value: value.encode("utf-8"),
                linger_ms=50,
                retries=5,
            )
        except Exception as error:
            print(f"Kafka пока недоступна: {error}")
            time.sleep(5)


def generate_event() -> dict:
    service = random.choice(SERVICES)
    is_error = random.random() < 0.12
    status_code = random.choice(ERROR_CODES) if is_error else random.choice([200, 201, 202, 204])

    base_latency = {
        "auth-service": 80,
        "payment-service": 180,
        "order-service": 140,
        "catalog-service": 95,
        "notification-service": 120,
    }[service]

    latency_ms = max(5, int(random.gauss(base_latency, base_latency * 0.35)))
    if is_error:
        latency_ms += random.randint(80, 700)

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "endpoint": random.choice(ENDPOINTS[service]),
        "method": random.choice(HTTP_METHODS),
        "status_code": status_code,
        "is_error": is_error,
        "latency_ms": latency_ms,
        "request_size_bytes": random.randint(120, 5000),
        "response_size_bytes": random.randint(200, 15000),
        "user_id": random.randint(1, 5000),
        "trace_id": str(uuid.uuid4()),
        "host": f"{service}-{random.randint(1, 4)}",
    }


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "microservice_logs")
    events_per_second = max(1, env_int("EVENTS_PER_SECOND", 5))
    sleep_interval = 1 / events_per_second

    producer = wait_for_kafka(bootstrap_servers)
    print(f"Генератор отправляет события в topic {topic}")

    while True:
        event = generate_event()
        producer.send(topic, key=event["service"], value=event)
        producer.flush()
        print(json.dumps(event, ensure_ascii=False))
        time.sleep(sleep_interval)


if __name__ == "__main__":
    main()

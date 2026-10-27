import json
import logging
import math
import os
from datetime import date, datetime, timedelta, timezone
from uuid import UUID
from zoneinfo import ZoneInfo

import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_values

from railway.config import DATABASE_URL, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from railway.predictor import DelayPredictor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EVENT_COLUMNS = (
    "event_id", "train_no", "event_timestamp", "service_date", "latitude", "longitude", "speed_kmph",
    "current_station_code", "next_station_code", "route_index", "progress_pct",
    "distance_to_next_km", "predicted_delay_minutes", "predicted_arrival",
    "prediction_source", "simulated", "priority_level", "overtaking",
)
POSITION_COLUMNS = (
    "train_no", "event_id", "event_timestamp", "service_date", "latitude", "longitude", "speed_kmph",
    "current_station_code", "next_station_code", "route_index", "progress_pct",
    "distance_to_next_km", "predicted_delay_minutes", "predicted_arrival",
    "prediction_source", "simulated", "priority_level", "overtaking",
)
INSERT_EVENT = """
    INSERT INTO train_events (
        event_id, train_no, event_timestamp, service_date, latitude, longitude, speed_kmph,
        current_station_code, next_station_code, route_index, progress_pct,
        distance_to_next_km, predicted_delay_minutes, predicted_arrival,
        prediction_source, simulated, priority_level, overtaking
    ) VALUES %s
    ON CONFLICT (event_id) DO NOTHING
"""

UPSERT_POSITION = """
    INSERT INTO train_positions (
        train_no, event_id, event_timestamp, service_date, latitude, longitude, speed_kmph,
        current_station_code, next_station_code, route_index, progress_pct,
        distance_to_next_km, predicted_delay_minutes, predicted_arrival,
        prediction_source, simulated, priority_level, overtaking
    ) VALUES %s
    ON CONFLICT (train_no) DO UPDATE SET
        event_id = EXCLUDED.event_id,
        event_timestamp = EXCLUDED.event_timestamp,
        service_date = EXCLUDED.service_date,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        speed_kmph = EXCLUDED.speed_kmph,
        current_station_code = EXCLUDED.current_station_code,
        next_station_code = EXCLUDED.next_station_code,
        route_index = EXCLUDED.route_index,
        progress_pct = EXCLUDED.progress_pct,
        distance_to_next_km = EXCLUDED.distance_to_next_km,
        predicted_delay_minutes = EXCLUDED.predicted_delay_minutes,
        predicted_arrival = EXCLUDED.predicted_arrival,
        prediction_source = EXCLUDED.prediction_source,
        simulated = EXCLUDED.simulated,
        priority_level = EXCLUDED.priority_level,
        overtaking = EXCLUDED.overtaking
    WHERE train_positions.event_timestamp < EXCLUDED.event_timestamp
"""


def validate_event(payload):
    required = (
        "event_id", "train_no", "event_timestamp", "latitude", "longitude",
        "speed_kmph", "current_station_code", "next_station_code", "route_index",
        "progress_pct", "distance_to_next_km", "distance_from_origin_km",
        "next_stop_no", "month", "day_of_week",
    )
    missing = [key for key in required if key not in payload]
    if missing:
        raise ValueError(f"Position event is missing required fields: {', '.join(missing)}")
    payload["event_id"] = str(UUID(payload["event_id"]))
    payload["train_no"] = str(int(payload["train_no"]))
    timestamp = datetime.fromisoformat(payload["event_timestamp"].replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError("event_timestamp must include a timezone")
    payload["event_timestamp"] = timestamp.astimezone(timezone.utc)
    payload["service_date"] = date.fromisoformat(
        str(
            payload.get("service_date")
            or timestamp.astimezone(ZoneInfo("Asia/Kolkata")).date()
        )
    )
    for field in (
        "latitude", "longitude", "speed_kmph", "progress_pct",
        "distance_to_next_km", "distance_from_origin_km",
    ):
        payload[field] = float(payload[field])
        if not math.isfinite(payload[field]):
            raise ValueError(f"Position event field {field} must be finite")
    for field in ("route_index", "next_stop_no", "month", "day_of_week"):
        payload[field] = int(payload[field])
    payload["priority_level"] = int(payload.get("priority_level", 2))
    if payload["priority_level"] not in (1, 2, 3):
        raise ValueError("priority_level must be 1, 2, or 3")
    payload["overtaking"] = bool(payload.get("overtaking", False))
    if not -90 <= payload["latitude"] <= 90 or not -180 <= payload["longitude"] <= 180:
        raise ValueError("Position event contains invalid coordinates")
    if payload["speed_kmph"] < 0 or payload["distance_to_next_km"] < 0:
        raise ValueError("Position event contains invalid speed or distance")
    if not 0 <= payload["progress_pct"] <= 100:
        raise ValueError("Position event contains invalid route progress")
    payload["simulated"] = bool(payload.get("simulated", True))
    return payload


def enrich_event(payload, predictor):
    event = validate_event(payload)
    delay_minutes = predictor.predict_delay_minutes(event)
    return enrich_validated_event(event, predictor, delay_minutes)


def enrich_events(payloads, predictor):
    events = [validate_event(payload) for payload in payloads]
    batch_predictor = getattr(predictor, "predict_delay_minutes_batch", None)
    delays = (
        batch_predictor(events)
        if callable(batch_predictor)
        else [predictor.predict_delay_minutes(event) for event in events]
    )
    if len(delays) != len(events):
        raise ValueError("The delay model returned an unexpected number of predictions")
    return [
        enrich_validated_event(event, predictor, delay)
        for event, delay in zip(events, delays)
    ]


def enrich_validated_event(event, predictor, delay_minutes):
    elapsed_seconds = event["event_timestamp"].timestamp()
    train_phase = sum(ord(character) for character in event["train_no"]) % 360
    variation = math.sin(
        2 * math.pi * elapsed_seconds / 60 + math.radians(train_phase)
    )
    simulated_on_time = int(event["train_no"]) % 5 == 0
    if simulated_on_time:
        adjusted_delay = 7.5 + 7.5 * variation
    else:
        adjusted_delay = max(-120.0, min(1440.0, delay_minutes + 8.0 * variation))
    eta_speed = event["speed_kmph"] or 30.0
    travel_minutes = event["distance_to_next_km"] / eta_speed * 60
    event["predicted_delay_minutes"] = adjusted_delay
    event["predicted_arrival"] = event["event_timestamp"] + timedelta(
        minutes=travel_minutes + max(0.0, adjusted_delay)
    )
    event["prediction_source"] = (
        f"{predictor.source}_simulated_on_time"
        if simulated_on_time
        else f"{predictor.source}_simulated"
    )
    return event


def run():
    predictor = DelayPredictor(
        os.getenv("MODEL_PATH", "/models/delay_model.json"),
        os.getenv("BASELINE_DELAY_MINUTES", "15"),
    )
    if predictor.source == "baseline":
        logger.warning(
            "Historical ML model not found; using the explicitly labeled %.1f-minute baseline. "
            "Run the Spark ETL and model-training steps to enable historical ML predictions.",
            predictor.baseline_delay_minutes,
        )
    with psycopg2.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE train_events
                    ADD COLUMN IF NOT EXISTS service_date DATE NOT NULL DEFAULT CURRENT_DATE,
                    ADD COLUMN IF NOT EXISTS priority_level INTEGER NOT NULL DEFAULT 2,
                    ADD COLUMN IF NOT EXISTS overtaking BOOLEAN NOT NULL DEFAULT FALSE;
                ALTER TABLE train_positions
                    ADD COLUMN IF NOT EXISTS service_date DATE NOT NULL DEFAULT CURRENT_DATE,
                    ADD COLUMN IF NOT EXISTS priority_level INTEGER NOT NULL DEFAULT 2,
                    ADD COLUMN IF NOT EXISTS overtaking BOOLEAN NOT NULL DEFAULT FALSE;
            """)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=os.getenv("KAFKA_GROUP_ID", "railway-prediction-service"),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        max_poll_records=500,
    )
    logger.info("Consuming Kafka topic %s with prediction source %s", KAFKA_TOPIC, predictor.source)
    with psycopg2.connect(DATABASE_URL) as connection:
        while True:
            polled_messages = consumer.poll(timeout_ms=1000, max_records=500)
            messages = [message for partition_messages in polled_messages.values() for message in partition_messages]
            if not messages:
                continue
            events = enrich_events([message.value for message in messages], predictor)
            event_values = [
                tuple(event[column] for column in EVENT_COLUMNS)
                for event in events
            ]
            latest_positions = {}
            for event in events:
                current = latest_positions.get(event["train_no"])
                if current is None or event["event_timestamp"] >= current["event_timestamp"]:
                    latest_positions[event["train_no"]] = event
            position_values = [
                tuple(event[column] for column in POSITION_COLUMNS)
                for event in latest_positions.values()
            ]
            with connection.cursor() as cursor:
                execute_values(cursor, INSERT_EVENT, event_values, page_size=500)
                execute_values(cursor, UPSERT_POSITION, position_values, page_size=500)
            connection.commit()
            consumer.commit()


if __name__ == "__main__":
    run()

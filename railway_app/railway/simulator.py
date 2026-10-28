import json
import logging
import math
import os
import random
import time
import uuid
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from zoneinfo import ZoneInfo

from kafka import KafkaProducer

from railway.catalog_loader import load_catalog
from railway.config import DATASET_ROOT, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, SIMULATION_SPEED

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def haversine_km(first, second):
    latitude_1, longitude_1 = first
    latitude_2, longitude_2 = second
    earth_radius_km = 6371.0
    latitude_delta = math.radians(latitude_2 - latitude_1)
    longitude_delta = math.radians(longitude_2 - longitude_1)
    value = (
        math.sin(latitude_delta / 2) ** 2
        + math.cos(math.radians(latitude_1))
        * math.cos(math.radians(latitude_2))
        * math.sin(longitude_delta / 2) ** 2
    )
    return 2 * earth_radius_km * math.asin(math.sqrt(value))


def build_routes(dataset_root):
    stations, trains, route_stops = load_catalog(dataset_root)
    coordinates = {
        station[0]: (station[5], station[6])
        for station in stations
        if station[5] is not None and station[6] is not None
    }
    train_info = {train[0]: {"name": train[1], "type": train[2]} for train in trains}
    grouped = {}
    for (
        train_no, stop_number, station_code, distance,
        arrival_day, arrival_time, departure_day, departure_time,
    ) in route_stops:
        location = coordinates.get(station_code)
        if location is None:
            continue
        grouped.setdefault(train_no, []).append({
            "stop_number": stop_number,
            "station_code": station_code,
            "distance_from_origin_km": distance,
            "arrival_day": arrival_day,
            "arrival_time": arrival_time,
            "departure_day": departure_day,
            "departure_time": departure_time,
            "latitude": location[0],
            "longitude": location[1],
        })

    routes = []
    for train_no, stops in grouped.items():
        stops.sort(key=lambda stop: stop["stop_number"])
        if len(stops) < 2:
            continue
        segments = []
        cumulative_distance = [0.0]
        for previous, following in zip(stops, stops[1:]):
            scheduled_distance = following["distance_from_origin_km"] - previous["distance_from_origin_km"]
            coordinate_distance = haversine_km(
                (previous["latitude"], previous["longitude"]),
                (following["latitude"], following["longitude"]),
            )
            segment_distance = scheduled_distance if scheduled_distance > 0 else coordinate_distance
            if segment_distance <= 0:
                segment_distance = 1.0
            segments.append(segment_distance)
            cumulative_distance.append(cumulative_distance[-1] + segment_distance)
        info = train_info.get(train_no, {"name": f"Train {train_no}", "type": None})
        routes.append({
            "train_no": train_no,
            "train_name": info["name"],
            "type_code": info["type"],
            "stops": stops,
            "segments_km": segments,
            "cumulative_km": cumulative_distance,
            "total_km": cumulative_distance[-1],
        })
    routes.sort(key=lambda route: route["train_no"])
    if not routes:
        raise RuntimeError("No train routes with at least two geocoded stops were found")
    return routes


def position_event(route, distance_km, speed_kmph, event_timestamp, service_date=None):
    total_distance = route["total_km"]
    distance_km %= total_distance
    segment_index = len(route["segments_km"]) - 1
    for index, segment_end in enumerate(route["cumulative_km"][1:]):
        if distance_km < segment_end:
            segment_index = index
            break
    segment_start_distance = route["cumulative_km"][segment_index]
    segment_length = route["segments_km"][segment_index]
    progress_in_segment = (distance_km - segment_start_distance) / segment_length
    previous = route["stops"][segment_index]
    following = route["stops"][segment_index + 1]
    latitude = previous["latitude"] + (following["latitude"] - previous["latitude"]) * progress_in_segment
    longitude = previous["longitude"] + (following["longitude"] - previous["longitude"]) * progress_in_segment
    distance_to_next = max(0.0, segment_length * (1 - progress_in_segment))
    route_index = following["stop_number"]
    event = {
        "event_id": str(uuid.uuid4()),
        "train_no": route["train_no"],
        "train_name": route["train_name"],
        "train_type": route["type_code"],
        "event_timestamp": event_timestamp.isoformat(),
        "latitude": latitude,
        "longitude": longitude,
        "speed_kmph": speed_kmph,
        "current_station_code": previous["station_code"],
        "next_station_code": following["station_code"],
        "route_index": route_index,
        "progress_pct": 100 * distance_km / total_distance,
        "distance_to_next_km": distance_to_next,
        "distance_from_origin_km": (
            previous["distance_from_origin_km"]
            + (following["distance_from_origin_km"] - previous["distance_from_origin_km"]) * progress_in_segment
        ),
        "next_stop_no": route_index,
        "month": event_timestamp.month,
        "day_of_week": event_timestamp.isoweekday(),
        "service_date": service_date.isoformat() if service_date else None,
        "simulated": True,
    }
    return event


def priority_level(route):
    name = route["train_name"].upper()
    if any(token in name for token in ("RAJDHANI", "SHATABDI", "DURONTO", "VANDE", "TEJAS", "GATIMAAN")):
        return 1
    if "EXPRESS" in name or route.get("type_code") == "EXP":
        return 2
    return 3


def speed_limit_kmph(priority):
    return 140.0 if priority == 1 else 100.0


def shared_segment_frequencies(routes):
    frequencies = {}
    for route in routes:
        for previous, following in zip(route["stops"], route["stops"][1:]):
            key = (previous["station_code"], following["station_code"])
            frequencies[key] = frequencies.get(key, 0) + 1
    return frequencies


def select_routes(routes, active_count, random_generator):
    segment_frequency = shared_segment_frequencies(routes)
    route_scores = {
        route["train_no"]: sum(
            max(0, segment_frequency[(previous["station_code"], following["station_code"])] - 1)
            for previous, following in zip(route["stops"], route["stops"][1:])
        )
        for route in routes
    }
    candidate_count = min(len(routes), max(active_count, active_count * 5))
    candidates = sorted(routes, key=lambda route: route_scores[route["train_no"]], reverse=True)[:candidate_count]
    priority_candidates = [route for route in routes if priority_level(route) == 1]
    priority_count = min(len(priority_candidates), active_count // 10)
    selected = random_generator.sample(priority_candidates, priority_count) if priority_count else []
    selected_numbers = {route["train_no"] for route in selected}
    remaining_candidates = [route for route in candidates if route["train_no"] not in selected_numbers]
    selected.extend(random_generator.sample(remaining_candidates, active_count - len(selected)))
    return selected


def shared_segment_status(selected_routes, progress, speeds, priorities):
    groups = {}
    segment_details = {}
    for route in selected_routes:
        train_no = route["train_no"]
        distance = progress[train_no] % route["total_km"]
        segment_index = len(route["segments_km"]) - 1
        for index, segment_end in enumerate(route["cumulative_km"][1:]):
            if distance < segment_end:
                segment_index = index
                break
        previous = route["stops"][segment_index]
        following = route["stops"][segment_index + 1]
        key = (previous["station_code"], following["station_code"])
        if key not in segment_details:
            segment_details[key] = (route["segments_km"][segment_index], route["cumulative_km"][segment_index])
        progress_on_segment = distance - route["cumulative_km"][segment_index]
        groups.setdefault(key, []).append((progress_on_segment, train_no))

    overtaking = set()
    for key, trains in groups.items():
        if len(trains) < 2:
            continue
        segment_length, _ = segment_details[key]
        ordered = sorted(trains)
        for (behind_distance, behind_train), (ahead_distance, ahead_train) in zip(ordered, ordered[1:]):
            gap_km = ahead_distance - behind_distance
            if priorities[behind_train] < priorities[ahead_train]:
                speeds[behind_train] = min(
                    speed_limit_kmph(priorities[behind_train]),
                    speeds[behind_train] * 1.4,
                )
                if gap_km <= min(5.0, segment_length / 3):
                    overtaking.add(behind_train)
    return overtaking


def _scheduled_datetime(stop, field, service_date):
    value = stop.get(f"{field}_time")
    if not value:
        return None
    try:
        stop_time = datetime_time.fromisoformat(value)
    except ValueError:
        return None
    service_day = stop.get(f"{field}_day") or 1
    return datetime.combine(service_date + timedelta(days=service_day - 1), stop_time)


def _simulated_speed_kmph(train_no, current_time, speed_limit):
    interval_seconds = 20
    seconds_today = (
        current_time.hour * 3600
        + current_time.minute * 60
        + current_time.second
        + current_time.microsecond / 1_000_000
    )
    interval = int(seconds_today // interval_seconds)
    interval_progress = (seconds_today % interval_seconds) / interval_seconds
    train_seed = sum((index + 1) * ord(character) for index, character in enumerate(str(train_no)))

    def target_for(bucket):
        generator = random.Random(
            f"{current_time.date().toordinal()}:{train_seed}:{bucket}"
        )
        return generator.uniform(8.0, speed_limit)

    previous_speed = target_for(interval - 1)
    target_speed = target_for(interval)
    eased_progress = (1 - math.cos(math.pi * interval_progress)) / 2
    return previous_speed + (target_speed - previous_speed) * eased_progress


def scheduled_route_state(route, current_time, speed_multiplier=1.0):
    if current_time.tzinfo is None:
        raise ValueError("current_time must include a timezone")
    if speed_multiplier <= 0:
        raise ValueError("speed_multiplier must be positive")
    local_time = current_time.astimezone(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)
    first_stop = route["stops"][0]
    last_stop = route["stops"][-1]
    maximum_trip_days = max(
        stop.get("arrival_day") or 1
        for stop in route["stops"]
    )
    for days_ago in range(maximum_trip_days + 1):
        service_date = local_time.date() - timedelta(days=days_ago)
        trip_start = _scheduled_datetime(first_stop, "departure", service_date)
        if trip_start is None:
            trip_start = _scheduled_datetime(first_stop, "arrival", service_date)
        trip_end = _scheduled_datetime(last_stop, "arrival", service_date)
        if trip_end is None:
            trip_end = _scheduled_datetime(last_stop, "departure", service_date)
        if trip_start is None or trip_end is None:
            continue
        simulated_trip_duration = (trip_end - trip_start) / speed_multiplier
        simulated_trip_end = trip_start + simulated_trip_duration
        if not trip_start <= local_time < simulated_trip_end:
            continue
        simulated_time = trip_start + (local_time - trip_start) * speed_multiplier

        for index, (previous, following) in enumerate(zip(route["stops"], route["stops"][1:])):
            departure = _scheduled_datetime(previous, "departure", service_date)
            if departure is None:
                departure = _scheduled_datetime(previous, "arrival", service_date)
            arrival = _scheduled_datetime(following, "arrival", service_date)
            if arrival is None:
                arrival = _scheduled_datetime(following, "departure", service_date)
            if departure is None or arrival is None:
                continue

            station_arrival = _scheduled_datetime(previous, "arrival", service_date)
            if station_arrival and station_arrival <= simulated_time < departure:
                distance = route["cumulative_km"][index]
                return {
                    "service_date": service_date,
                    "distance_km": distance,
                    "speed_kmph": 0.0,
                }
            if departure <= simulated_time <= arrival:
                duration_seconds = (arrival - departure).total_seconds()
                if duration_seconds <= 0:
                    continue
                progress = (simulated_time - departure).total_seconds() / duration_seconds
                segment_length = route["segments_km"][index]
                return {
                    "service_date": service_date,
                    "distance_km": route["cumulative_km"][index] + segment_length * progress,
                    "speed_kmph": _simulated_speed_kmph(
                        route["train_no"],
                        simulated_time,
                        speed_limit_kmph(priority_level(route)),
                    ),
                }
    return None


def run():
    routes = build_routes(DATASET_ROOT)
    active_limit = int(os.getenv("ACTIVE_TRAINS", "200"))
    if active_limit < 1:
        raise ValueError("ACTIVE_TRAINS must be at least 1")
    interval_seconds = float(os.getenv("EVENT_INTERVAL_SECONDS", "2"))
    if interval_seconds <= 0:
        raise ValueError("EVENT_INTERVAL_SECONDS must be positive")
    simulation_speed = SIMULATION_SPEED
    if simulation_speed <= 0:
        raise ValueError("SIMULATION_SPEED must be positive")
    random_generator = random.Random(42)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=10,
    )
    logger.info("Checking today's scheduled train services against the supplied timetable")
    last_published_count = None
    selected_train_numbers = set()
    while True:
        cycle_started = time.monotonic()
        timestamp = datetime.now(timezone.utc)
        local_now = datetime.now(ZoneInfo("Asia/Kolkata"))
        active_states = {
            route["train_no"]: scheduled_route_state(route, local_now, simulation_speed)
            for route in routes
        }
        scheduled_routes = [route for route in routes if active_states[route["train_no"]] is not None]
        currently_active = {route["train_no"] for route in scheduled_routes}
        selected_train_numbers.intersection_update(currently_active)
        selected_routes = [
            route for route in scheduled_routes
            if route["train_no"] in selected_train_numbers
        ]
        new_route_count = min(active_limit, len(scheduled_routes)) - len(selected_routes)
        if new_route_count > 0:
            candidates = [
                route for route in scheduled_routes
                if route["train_no"] not in selected_train_numbers
            ]
            new_routes = select_routes(candidates, new_route_count, random_generator)
            selected_routes.extend(new_routes)
            selected_train_numbers.update(route["train_no"] for route in new_routes)
        priorities = {route["train_no"]: priority_level(route) for route in selected_routes}
        progress = {
            route["train_no"]: active_states[route["train_no"]]["distance_km"]
            for route in selected_routes
        }
        speeds = {
            route["train_no"]: active_states[route["train_no"]]["speed_kmph"]
            for route in selected_routes
        }
        overtaking = shared_segment_status(selected_routes, progress, speeds, priorities)
        for route in selected_routes:
            train_no = route["train_no"]
            state = active_states[train_no]
            event = position_event(route, state["distance_km"], speeds[train_no], timestamp, state["service_date"])
            event["priority_level"] = priorities[train_no]
            event["overtaking"] = train_no in overtaking
            producer.send(KAFKA_TOPIC, key=train_no, value=event)
        if len(selected_routes) != last_published_count:
            logger.info(
                "Publishing positions for %d of %d services scheduled now to Kafka topic %s",
                len(selected_routes),
                len(scheduled_routes),
                KAFKA_TOPIC,
            )
            last_published_count = len(selected_routes)
        producer.flush(timeout=10)
        sleep_for = interval_seconds - (time.monotonic() - cycle_started)
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    run()

import csv
import json
import logging
import os
from collections import defaultdict

import psycopg2
from psycopg2.extras import execute_values

from railway.config import DATASET_ROOT, DATABASE_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def canonical_train_number(value):
    number = str(value).strip()
    if not number.isdigit():
        raise ValueError(f"Train number must be numeric, got {value!r}")
    return str(int(number))


def load_catalog(dataset_root):
    network_dir = os.path.join(dataset_root, "Railway_Network")
    delay_dir = os.path.join(dataset_root, "Railway_delays")

    full_station_names = {}
    full_names_path = os.path.join(delay_dir, "station_full_names.csv")
    if os.path.exists(full_names_path):
        with open(full_names_path, newline="", encoding="utf-8") as stream:
            for row in csv.DictReader(stream):
                code = (row.get("station_name") or "").strip()
                name = (row.get("station_full_name") or "").strip()
                if code and name:
                    full_station_names[code] = name

    with open(os.path.join(network_dir, "stations.json"), encoding="utf-8") as stream:
        station_features = json.load(stream)["features"]

    stations = []
    station_codes = set()
    for feature in station_features:
        properties = feature.get("properties") or {}
        geometry = feature.get("geometry") or {}
        coordinates = geometry.get("coordinates") if geometry.get("type") == "Point" else None
        longitude, latitude = coordinates if coordinates and len(coordinates) >= 2 else (None, None)
        code = properties.get("code")
        if not code:
            continue
        code = code.strip()
        station_codes.add(code)
        stations.append((
            code,
            full_station_names.get(code) or properties.get("name") or code,
            properties.get("state"),
            properties.get("zone"),
            properties.get("address"),
            latitude,
            longitude,
        ))
    stations.extend(
        (code, name, None, None, None, None, None)
        for code, name in full_station_names.items()
        if code not in station_codes
    )

    train_details = {}
    with open(os.path.join(delay_dir, "train_details.csv"), newline="", encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            train_no = canonical_train_number(row["train_no"])
            train_details.setdefault(train_no, (row["train_name"].strip(), row["type_code"].strip()))

    grouped_stops = defaultdict(dict)
    with open(os.path.join(delay_dir, "combined_schedule.csv"), newline="", encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            train_no = canonical_train_number(row["train_no"])
            station_code = row["station_name"].strip()
            if not station_code:
                continue
            grouped_stops[train_no][int(row["station_no"])] = (
                int(row["station_no"]),
                station_code,
                float(row["distance_from_origin"] or 0),
                int(row["arrival_day"]) if row["arrival_day"] else None,
                row["arrival_time"] or None,
                int(row["departure_day"]) if row["departure_day"] else None,
                row["departure_time"] or None,
            )

    trains = []
    route_stops = []
    for train_no, stops_by_number in grouped_stops.items():
        stops = list(stops_by_number.values())
        stops.sort(key=lambda stop: stop[0])
        name, type_code = train_details.get(train_no, (f"Train {train_no}", None))
        trains.append((train_no, name, type_code))
        route_stops.extend(
            (train_no, stop_number, station_code, distance, arrival_day, arrival_time, departure_day, departure_time)
            for stop_number, station_code, distance, arrival_day, arrival_time, departure_day, departure_time in stops
        )
    return stations, trains, route_stops


def persist_catalog(connection, stations, trains, route_stops):
    with connection.cursor() as cursor:
        execute_values(
            cursor,
            """
            INSERT INTO stations
                (station_code, station_name, state, zone, address, latitude, longitude)
            VALUES %s
            ON CONFLICT (station_code) DO UPDATE SET
                station_name = EXCLUDED.station_name,
                state = EXCLUDED.state,
                zone = EXCLUDED.zone,
                address = EXCLUDED.address,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude
            """,
            stations,
            page_size=1000,
        )
        execute_values(
            cursor,
            """
            INSERT INTO trains (train_no, train_name, type_code)
            VALUES %s
            ON CONFLICT (train_no) DO UPDATE SET
                train_name = EXCLUDED.train_name,
                type_code = EXCLUDED.type_code
            """,
            trains,
            page_size=1000,
        )
        execute_values(
            cursor,
            """
            INSERT INTO route_stops
                (train_no, stop_number, station_code, distance_from_origin_km,
                 arrival_day, arrival_time, departure_day, departure_time)
            VALUES %s
            ON CONFLICT (train_no, stop_number) DO UPDATE SET
                station_code = EXCLUDED.station_code,
                distance_from_origin_km = EXCLUDED.distance_from_origin_km,
                arrival_day = EXCLUDED.arrival_day,
                arrival_time = EXCLUDED.arrival_time,
                departure_day = EXCLUDED.departure_day,
                departure_time = EXCLUDED.departure_time
            """,
            route_stops,
            page_size=2000,
        )


def main():
    stations, trains, route_stops = load_catalog(DATASET_ROOT)
    if not stations or not trains or not route_stops:
        raise RuntimeError(f"No usable railway catalog found under {DATASET_ROOT}")
    with psycopg2.connect(DATABASE_URL) as connection:
        persist_catalog(connection, stations, trains, route_stops)
    logger.info(
        "Loaded %d stations, %d trains and %d route stops",
        len(stations),
        len(trains),
        len(route_stops),
    )


if __name__ == "__main__":
    main()

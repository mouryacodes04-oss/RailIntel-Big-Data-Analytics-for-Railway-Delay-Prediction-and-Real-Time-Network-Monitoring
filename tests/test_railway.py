import csv
import json
import random
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from railway.catalog_loader import canonical_train_number, load_catalog
from railway.simulator import (
    build_routes,
    position_event,
    priority_level,
    scheduled_route_state,
    select_routes,
    speed_limit_kmph,
    shared_segment_status,
)
from railway.predictor import DelayPredictor
from railway.stream_processor import enrich_event, validate_event


class RailwayPipelineTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        network = self.root / "Railway_Network"
        delays = self.root / "Railway_delays"
        network.mkdir()
        delays.mkdir()

        station_features = []
        for code, name, longitude, latitude in (
            ("AAA", "Alpha", 77.0, 12.0),
            ("BBB", "Beta", 77.1, 12.0),
            ("CCC", "Gamma", 77.2, 12.0),
        ):
            station_features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [longitude, latitude]},
                "properties": {
                    "code": code,
                    "name": name,
                    "state": "Test",
                    "zone": "TZ",
                    "address": None,
                },
            })
        (network / "stations.json").write_text(
            json.dumps({"type": "FeatureCollection", "features": station_features}),
            encoding="utf-8",
        )
        with (delays / "train_details.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=["train_no", "train_name", "type_code"])
            writer.writeheader()
            writer.writerow({"train_no": "00123", "train_name": "Test Express", "type_code": "EXP"})
        with (delays / "combined_schedule.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=[
                "station_no", "station_name", "distance_from_origin", "arrival_day",
                "arrival_time", "departure_day", "departure_time", "train_no",
            ])
            writer.writeheader()
            for index, station in enumerate(("AAA", "BBB", "CCC"), start=1):
                writer.writerow({
                    "station_no": index,
                    "station_name": station,
                    "distance_from_origin": (index - 1) * 10,
                    "arrival_day": 1,
                    "arrival_time": {1: "", 2: "08:30", 3: "09:00"}[index],
                    "departure_day": 1,
                    "departure_time": {1: "08:00", 2: "08:40", 3: ""}[index],
                    "train_no": "00123",
                })

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_train_number_normalization(self):
        self.assertEqual(canonical_train_number("00123"), "123")
        with self.assertRaises(ValueError):
            canonical_train_number("12A")

    def test_catalog_and_route_interpolation(self):
        stations, trains, stops = load_catalog(str(self.root))
        self.assertEqual(len(stations), 3)
        self.assertEqual(trains, [("123", "Test Express", "EXP")])
        self.assertEqual(len(stops), 3)

        route = build_routes(str(self.root))[0]
        event_time = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
        event = position_event(route, 5, 50, event_time)
        self.assertEqual(event["current_station_code"], "AAA")
        self.assertEqual(event["next_station_code"], "BBB")
        self.assertEqual(event["route_index"], 2)
        self.assertAlmostEqual(event["latitude"], 12.0)
        self.assertAlmostEqual(event["longitude"], 77.05)
        self.assertAlmostEqual(event["distance_to_next_km"], 5)
        self.assertAlmostEqual(event["progress_pct"], 25)

    def test_scheduled_route_state_uses_timetable_and_service_day(self):
        route = build_routes(str(self.root))[0]
        current_time = datetime(2026, 1, 2, 8, 15, tzinfo=ZoneInfo("Asia/Kolkata"))
        state = scheduled_route_state(route, current_time)
        self.assertEqual(state["service_date"].isoformat(), "2026-01-02")
        self.assertAlmostEqual(state["distance_km"], 5)
        self.assertGreaterEqual(state["speed_kmph"], 0)
        self.assertLessEqual(state["speed_kmph"], 100)
        accelerated_state = scheduled_route_state(
            route,
            datetime(2026, 1, 2, 8, 5, tzinfo=ZoneInfo("Asia/Kolkata")),
            speed_multiplier=4,
        )
        self.assertAlmostEqual(accelerated_state["distance_km"], 6.6667, places=3)
        self.assertGreaterEqual(accelerated_state["speed_kmph"], 8)
        self.assertLessEqual(accelerated_state["speed_kmph"], 100)
        high_priority_route = {**route, "train_name": "Test Rajdhani"}
        high_priority_state = scheduled_route_state(
            high_priority_route,
            datetime(2026, 1, 2, 8, 5, tzinfo=ZoneInfo("Asia/Kolkata")),
            speed_multiplier=4,
        )
        self.assertEqual(speed_limit_kmph(priority_level(high_priority_route)), 140)
        self.assertGreaterEqual(high_priority_state["speed_kmph"], 8)
        self.assertLessEqual(high_priority_state["speed_kmph"], 140)
        stopped_state = scheduled_route_state(
            route,
            datetime(2026, 1, 2, 8, 35, tzinfo=ZoneInfo("Asia/Kolkata")),
        )
        self.assertEqual(stopped_state["speed_kmph"], 0)
        self.assertIsNone(
            scheduled_route_state(
                route,
                datetime(2026, 1, 2, 10, 0, tzinfo=ZoneInfo("Asia/Kolkata")),
            )
        )
        in_motion = datetime(2026, 1, 2, 8, 20, tzinfo=ZoneInfo("Asia/Kolkata"))
        speeds = [
            scheduled_route_state(route, in_motion + timedelta(seconds=seconds))["speed_kmph"]
            for seconds in range(0, 180, 5)
        ]
        self.assertGreater(max(speeds) - min(speeds), 40)
        self.assertTrue(any(later > earlier for earlier, later in zip(speeds, speeds[1:])))
        self.assertTrue(any(later < earlier for earlier, later in zip(speeds, speeds[1:])))

    def test_shared_corridor_priority_overtake(self):
        route = build_routes(str(self.root))[0]
        following_train = {**route, "train_no": "124"}
        routes = [route, following_train]
        selected = select_routes(routes, 2, random.Random(42))
        self.assertEqual({item["train_no"] for item in selected}, {"123", "124"})

        speeds = {"123": 60.0, "124": 40.0}
        overtaking = shared_segment_status(
            routes,
            {"123": 2.0, "124": 5.0},
            speeds,
            {"123": 1, "124": 3},
        )
        self.assertIn("123", overtaking)
        self.assertGreater(speeds["123"], 60.0)
        self.assertEqual(priority_level(route), 2)
        capped_speeds = {"123": 95.0, "124": 40.0}
        shared_segment_status(
            routes,
            {"123": 2.0, "124": 5.0},
            capped_speeds,
            {"123": 2, "124": 3},
        )
        self.assertEqual(capped_speeds["123"], 100.0)

    def test_event_validation_and_eta_enrichment(self):
        route = build_routes(str(self.root))[0]
        timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
        payload = position_event(route, 5, 50, timestamp)
        stopped_payload = position_event(route, 5, 0, timestamp)
        self.assertEqual(validate_event(stopped_payload)["speed_kmph"], 0)
        with self.assertRaisesRegex(ValueError, "missing required fields"):
            validate_event({"train_no": "123"})

        class Predictor:
            source = "historical_ml"

            @staticmethod
            def predict_delay_minutes(event):
                return 7.0

        enriched = enrich_event(payload, Predictor())
        self.assertEqual(enriched["prediction_source"], "historical_ml_simulated")
        self.assertNotEqual(enriched["predicted_delay_minutes"], 7)
        self.assertEqual(enriched["service_date"].isoformat(), "2026-01-02")
        self.assertAlmostEqual(
            (enriched["predicted_arrival"] - timestamp).total_seconds() / 60,
            6.0 + max(0.0, enriched["predicted_delay_minutes"]),
        )
        later_payload = position_event(route, 5, 50, timestamp + timedelta(seconds=15))
        later = enrich_event(later_payload, Predictor())
        self.assertNotEqual(
            enriched["predicted_delay_minutes"],
            later["predicted_delay_minutes"],
        )

        on_time_payload = {
            **payload,
            "train_no": "125",
            "event_timestamp": timestamp.isoformat(),
        }
        on_time = enrich_event(on_time_payload, Predictor())
        self.assertIn("_simulated_on_time", on_time["prediction_source"])
        self.assertGreaterEqual(on_time["predicted_delay_minutes"], 0)
        self.assertLessEqual(on_time["predicted_delay_minutes"], 15)

    def test_spark_mllib_json_model_prediction_and_legacy_fallback(self):
        model_path = self.root / "delay_model.json"
        model_path.write_text(json.dumps({
            "format": "railway-linear-regression-v1",
            "model": "Spark MLlib LinearRegression",
            "numeric_features": ["station_no", "distance_from_origin_km"],
            "categorical_features": ["train_no", "station_code"],
            "category_indexes": {
                "train_no": {"123": 0, "456": 1},
                "station_code": {"BBB": 0, "CCC": 1},
            },
            "category_sizes": {"train_no": 3, "station_code": 3},
            "coefficients": [1.5, 0.1, 2.0, -1.0, 0.5, 3.0, -2.0, 1.0],
            "intercept": 4.0,
        }), encoding="utf-8")
        predictor = DelayPredictor(model_path, 15)
        prediction = predictor.predict_delay_minutes({
            "next_stop_no": 2,
            "distance_from_origin_km": 10,
            "train_no": "123",
            "next_station_code": "BBB",
        })
        self.assertEqual(predictor.source, "historical_ml")
        self.assertAlmostEqual(prediction, 13.0)

        unknown_prediction = predictor.predict_delay_minutes({
            "next_stop_no": 2,
            "distance_from_origin_km": 10,
            "train_no": "unknown",
            "next_station_code": "ZZZ",
        })
        self.assertAlmostEqual(unknown_prediction, 9.5)

        model_path.write_text(json.dumps({
            "format": "railway-linear-regression-v1",
            "numeric_features": ["station_no"],
            "categorical_features": [],
            "category_indexes": {},
            "category_sizes": {},
            "coefficients": [],
            "intercept": 0.0,
        }), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "coefficient count"):
            DelayPredictor(model_path, 15)

        legacy_predictor = DelayPredictor(self.root / "missing_model.json", 15)
        self.assertEqual(legacy_predictor.source, "baseline")
        self.assertEqual(legacy_predictor.predict_delay_minutes({}), 15)

        class LegacyModel:
            def predict(self, features):
                return [17.0] * len(features)

        legacy_path = self.root / "legacy_model.json"
        legacy_path.with_suffix(".joblib").touch()
        with patch("railway.predictor.joblib.load", return_value=LegacyModel()):
            legacy_predictor = DelayPredictor(legacy_path, 15)
        self.assertEqual(legacy_predictor.source, "historical_ml")
        self.assertEqual(
            legacy_predictor.predict_delay_minutes({
                "next_stop_no": 2,
                "distance_from_origin_km": 10,
                "month": 1,
                "day_of_week": 2,
                "train_no": "123",
                "next_station_code": "BBB",
            }),
            17.0,
        )


if __name__ == "__main__":
    unittest.main()

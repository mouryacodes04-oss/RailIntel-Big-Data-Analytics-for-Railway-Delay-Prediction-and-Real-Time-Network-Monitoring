import json
import math
from pathlib import Path

import joblib
import pandas as pd


FEATURE_COLUMNS = [
    "station_no",
    "distance_from_origin_km",
    "month",
    "day_of_week",
    "train_no",
    "station_code",
    "train_type",
]


class DelayPredictor:
    def __init__(self, model_path, baseline_delay_minutes):
        self.model = None
        self.spark_model = None
        model_file = Path(model_path)
        if model_file.exists() and model_file.suffix == ".json":
            with model_file.open(encoding="utf-8") as stream:
                self.spark_model = json.load(stream)
            self._validate_spark_model()
        elif model_file.exists():
            self.model = joblib.load(model_file)
        elif model_file.suffix == ".json":
            legacy_model = model_file.with_suffix(".joblib")
            if legacy_model.exists():
                self.model = joblib.load(legacy_model)
        self.baseline_delay_minutes = float(baseline_delay_minutes)

    def _validate_spark_model(self):
        model = self.spark_model
        if model.get("format") != "railway-linear-regression-v1":
            raise ValueError("Unsupported Spark MLlib delay model artifact format")
        numeric_features = model.get("numeric_features")
        categorical_features = model.get("categorical_features")
        category_sizes = model.get("category_sizes")
        category_indexes = model.get("category_indexes")
        coefficients = model.get("coefficients")
        if not isinstance(numeric_features, list) or not isinstance(categorical_features, list):
            raise ValueError("Spark MLlib model is missing its feature definitions")
        if not isinstance(category_sizes, dict) or not isinstance(category_indexes, dict):
            raise ValueError("Spark MLlib model is missing its categorical encodings")
        expected_size = len(numeric_features)
        for feature in categorical_features:
            size = category_sizes.get(feature)
            indexes = category_indexes.get(feature)
            if not isinstance(size, int) or size < 1 or not isinstance(indexes, dict):
                raise ValueError(f"Spark MLlib model has an invalid category map for {feature}")
            if any(not isinstance(index, int) or index < 0 or index >= size - 1
                   for index in indexes.values()):
                raise ValueError(f"Spark MLlib model has an out-of-range category index for {feature}")
            expected_size += size
        if not isinstance(coefficients, list) or len(coefficients) != expected_size:
            raise ValueError("Spark MLlib model coefficient count does not match its feature layout")
        if any(not math.isfinite(float(value)) for value in coefficients):
            raise ValueError("Spark MLlib model contains a non-finite coefficient")
        if not math.isfinite(float(model.get("intercept", float("nan")))):
            raise ValueError("Spark MLlib model contains an invalid intercept")

    @property
    def source(self):
        return "historical_ml" if self.model is not None or self.spark_model is not None else "baseline"

    def predict_delay_minutes(self, event):
        if self.model is None and self.spark_model is None:
            return self.baseline_delay_minutes
        return self.predict_delay_minutes_batch([event])[0]

    def predict_delay_minutes_batch(self, events):
        if self.spark_model is not None:
            return self._predict_spark_model_batch(events)
        if self.model is None:
            return [self.baseline_delay_minutes] * len(events)
        features = pd.DataFrame(
            [{
                "station_no": int(event["next_stop_no"]),
                "distance_from_origin_km": float(event["distance_from_origin_km"]),
                "month": int(event["month"]),
                "day_of_week": int(event["day_of_week"]),
                "train_no": str(event["train_no"]),
                "station_code": event["next_station_code"],
                "train_type": event.get("train_type") or "UNKNOWN",
            } for event in events],
            columns=FEATURE_COLUMNS,
        )
        predictions = [float(value) for value in self.model.predict(features)]
        if any(not math.isfinite(prediction) for prediction in predictions):
            raise ValueError("The delay model returned a non-finite prediction")
        return [max(-120.0, min(1440.0, prediction)) for prediction in predictions]

    def _predict_spark_model_batch(self, events):
        model = self.spark_model
        numeric_features = model["numeric_features"]
        categorical_features = model["categorical_features"]
        category_sizes = model["category_sizes"]
        category_indexes = model["category_indexes"]
        coefficients = model["coefficients"]
        predictions = []
        for event in events:
            prediction = float(model["intercept"])
            for index, feature in enumerate(numeric_features):
                if feature == "station_no":
                    value = event.get("next_stop_no", 0)
                elif feature == "station_code":
                    value = event.get("next_station_code") or "UNKNOWN"
                elif feature == "train_type":
                    value = event.get("train_type") or "UNKNOWN"
                else:
                    value = event.get(feature, 0)
                prediction += float(coefficients[index]) * float(value)

            offset = len(numeric_features)
            for feature in categorical_features:
                value = event.get(feature)
                if feature == "station_code":
                    value = event.get("next_station_code")
                elif feature == "train_type":
                    value = event.get("train_type")
                index = category_indexes[feature].get(
                    str(value or "UNKNOWN"),
                    category_sizes[feature] - 1,
                )
                prediction += float(coefficients[offset + index])
                offset += category_sizes[feature]
            if not math.isfinite(prediction):
                raise ValueError("The Spark MLlib model returned a non-finite prediction")
            predictions.append(max(-120.0, min(1440.0, prediction)))
        return predictions

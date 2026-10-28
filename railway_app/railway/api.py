from contextlib import closing
from datetime import date, datetime, time as datetime_time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg2
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from psycopg2.extras import RealDictCursor

from railway.config import DATABASE_URL, SIMULATION_SPEED

app = FastAPI(title="Railway Live Analytics API", version="1.0.0")
INDEX_FILE = Path(__file__).resolve().parent.parent / "static" / "index.html"
INDIA_TZ = ZoneInfo("Asia/Kolkata")


@app.on_event("startup")
def ensure_simulation_columns():
    with closing(psycopg2.connect(DATABASE_URL)) as connection:
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
        connection.commit()


def query_rows(sql, parameters=None):
    with closing(psycopg2.connect(DATABASE_URL)) as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql, parameters or ())
            return [dict(row) for row in cursor.fetchall()]


def scheduled_stop_datetime(stop, field, service_date):
    raw_time = stop.get(f"{field}_time")
    if not raw_time:
        return None
    try:
        stop_time = datetime_time.fromisoformat(raw_time)
    except ValueError:
        return None
    stop_day = stop.get(f"{field}_day") or 1
    return datetime.combine(
        service_date + timedelta(days=stop_day - 1),
        stop_time,
        tzinfo=INDIA_TZ,
    )


def simulated_station_arrival(stop, first_stop, service_date, speed_multiplier):
    if speed_multiplier <= 0:
        raise ValueError("SIMULATION_SPEED must be positive")
    station_arrival = scheduled_stop_datetime(stop, "arrival", service_date)
    trip_start = (
        scheduled_stop_datetime(first_stop, "departure", service_date)
        or scheduled_stop_datetime(first_stop, "arrival", service_date)
    )
    if station_arrival is None or trip_start is None or station_arrival < trip_start:
        return None
    return trip_start + (station_arrival - trip_start) / speed_multiplier


def expected_time_with_delay(scheduled_time, delay_minutes):
    if scheduled_time is None or delay_minutes is None:
        return None
    return scheduled_time + timedelta(minutes=float(delay_minutes))


@app.get("/", include_in_schema=False)
@app.get("/journeys", include_in_schema=False)
@app.get("/station-arrivals", include_in_schema=False)
@app.get("/live-status", include_in_schema=False)
@app.get("/analytics", include_in_schema=False)
def dashboard():
    return FileResponse(INDEX_FILE)


@app.get("/api/health")
def health():
    query_rows("SELECT 1 AS database")
    return {"status": "ok"}


@app.get("/api/stats")
def stats():
    rows = query_rows(
        """
        SELECT
            COUNT(*) AS active_trains,
            COALESCE(ROUND(AVG(predicted_delay_minutes)::numeric, 1), 0) AS average_predicted_delay_minutes,
            COUNT(*) FILTER (WHERE LEFT(prediction_source, 13) = 'historical_ml') AS ml_predictions,
            COUNT(*) FILTER (WHERE LEFT(prediction_source, 8) = 'baseline') AS baseline_predictions,
            COUNT(*) FILTER (WHERE predicted_delay_minutes > 15) AS delayed_trains,
            COUNT(*) FILTER (WHERE predicted_delay_minutes <= 15) AS on_time_or_early_trains,
            MAX(event_timestamp) AS latest_update
        FROM train_positions
        WHERE event_timestamp >= NOW() - INTERVAL '15 seconds'
        """
    )
    return rows[0]


@app.get("/api/trains")
def trains(limit: int = Query(default=200, ge=1, le=1000)):
    return query_rows(
        """
        SELECT p.train_no, t.train_name, t.type_code, p.event_timestamp, p.service_date,
               p.latitude, p.longitude, p.speed_kmph,
               p.current_station_code, current_station.station_name AS current_station_name,
               p.next_station_code, next_station.station_name AS next_station_name,
               p.route_index, p.progress_pct, p.distance_to_next_km,
               p.predicted_delay_minutes, p.predicted_arrival, p.prediction_source,
               p.simulated, p.priority_level, p.overtaking
        FROM train_positions p
        JOIN trains t ON t.train_no = p.train_no
        LEFT JOIN stations current_station ON current_station.station_code = p.current_station_code
        LEFT JOIN stations next_station ON next_station.station_code = p.next_station_code
        WHERE p.event_timestamp >= NOW() - INTERVAL '15 seconds'
        ORDER BY p.event_timestamp DESC
        LIMIT %s
        """,
        (limit,),
    )


@app.get("/api/stations")
def stations(
    q: str = Query(default="", max_length=100),
    limit: int = Query(default=5000, ge=1, le=10000),
):
    search = q.strip()
    if search:
        return query_rows(
            """
            SELECT station_code, station_name, state, zone
            FROM stations
            WHERE station_code ILIKE %s OR station_name ILIKE %s
            ORDER BY station_name, station_code
            LIMIT %s
            """,
            (f"%{search}%", f"%{search}%", limit),
        )
    return query_rows(
        """
        SELECT station_code, station_name, state, zone
        FROM stations
        ORDER BY station_name, station_code
        LIMIT %s
        """,
        (limit,),
    )


@app.get("/api/trains/between")
def trains_between(
    from_station: str = Query(min_length=1, max_length=50),
    to_station: str = Query(min_length=1, max_length=50),
    service_day: date | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=300),
):
    if from_station.upper() == to_station.upper():
        raise HTTPException(status_code=400, detail="Choose two different stations")
    service_day = service_day or datetime.now(INDIA_TZ).date()
    rows = query_rows(
        """
        WITH origins AS (
            SELECT DISTINCT ON (train_no) train_no, stop_number
            FROM route_stops
            WHERE station_code = %s
            ORDER BY train_no, stop_number
        ),
        matches AS (
            SELECT o.train_no, o.stop_number AS from_stop, d.stop_number AS to_stop
            FROM origins o
            JOIN LATERAL (
                SELECT stop_number
                FROM route_stops
                WHERE train_no = o.train_no AND station_code = %s
                  AND stop_number > o.stop_number
                ORDER BY stop_number
                LIMIT 1
            ) d ON TRUE
            ORDER BY o.train_no
            LIMIT %s
        )
        SELECT m.train_no, t.train_name, t.type_code,
               r.stop_number, r.station_code, s.station_name,
               r.arrival_day, r.arrival_time, r.departure_day, r.departure_time
        FROM matches m
        JOIN trains t ON t.train_no = m.train_no
        JOIN route_stops r ON r.train_no = m.train_no
            AND r.stop_number IN (m.from_stop, m.to_stop)
        LEFT JOIN stations s ON s.station_code = r.station_code
        ORDER BY m.train_no, r.stop_number
        """,
        (from_station.strip().upper(), to_station.strip().upper(), limit),
    )
    result = {}
    for row in rows:
        train = result.setdefault(row["train_no"], {
            "train_no": row["train_no"],
            "train_name": row["train_name"],
            "type_code": row["type_code"],
            "stops": [],
        })
        train["stops"].append({
            "stop_number": row["stop_number"],
            "station_code": row["station_code"],
            "station_name": row["station_name"],
            "scheduled_arrival": scheduled_stop_datetime(row, "arrival", service_day),
            "scheduled_departure": scheduled_stop_datetime(row, "departure", service_day),
        })
    return {
        "from_station": from_station.strip().upper(),
        "to_station": to_station.strip().upper(),
        "service_day": service_day,
        "service_day_assumption": "Daily service; the source timetable contains no weekday-of-operation fields.",
        "trains": list(result.values()),
    }


@app.get("/api/stations/{station_code}/arrivals")
def station_arrivals(
    station_code: str,
    limit: int = Query(default=1000, ge=1, le=5000),
):
    station_code = station_code.strip().upper()
    window_start = datetime.now(INDIA_TZ)
    window_end = window_start + timedelta(hours=4)
    rows = query_rows(
        """
        WITH target AS (
            SELECT %s::text AS station_code,
                   %s::timestamptz AT TIME ZONE 'Asia/Kolkata' AS window_start,
                   %s::timestamptz AT TIME ZONE 'Asia/Kolkata' AS window_end
        ),
        candidate_dates AS (
            SELECT target.*,
                   candidate.arrival_date::date AS arrival_date
            FROM target
            CROSS JOIN LATERAL generate_series(
                date_trunc('day', target.window_start),
                date_trunc('day', target.window_end),
                INTERVAL '1 day'
            ) AS candidate(arrival_date)
        ),
        scheduled AS (
            SELECT target.window_start, target.window_end, target.arrival_date,
                   t.train_no, t.train_name, t.type_code, r.stop_number,
                   s.station_code, s.station_name,
                   COALESCE(r.arrival_day, r.departure_day, 1) AS timetable_day,
                   COALESCE(r.arrival_time, r.departure_time) AS timetable_time,
                   (target.arrival_date - (COALESCE(r.arrival_day, r.departure_day, 1) - 1)) AS origin_service_date,
                   previous.station_code AS previous_station_code,
                   previous_station.station_name AS previous_station_name
            FROM candidate_dates target
            JOIN route_stops r ON r.station_code = target.station_code
            JOIN trains t ON t.train_no = r.train_no
            JOIN stations s ON s.station_code = r.station_code
            LEFT JOIN LATERAL (
                SELECT station_code
                FROM route_stops
                WHERE train_no = r.train_no AND stop_number < r.stop_number
                ORDER BY stop_number DESC
                LIMIT 1
            ) previous ON TRUE
            LEFT JOIN stations previous_station ON previous_station.station_code = previous.station_code
            WHERE COALESCE(r.arrival_time, r.departure_time) IS NOT NULL
        ),
        arrivals AS (
            SELECT scheduled.*,
                   (scheduled.arrival_date + scheduled.timetable_time::time)
                       AT TIME ZONE 'Asia/Kolkata' AS scheduled_arrival
            FROM scheduled
            WHERE (scheduled.arrival_date + scheduled.timetable_time::time) >= scheduled.window_start
              AND (scheduled.arrival_date + scheduled.timetable_time::time) < scheduled.window_end
        )
        SELECT arrivals.train_no, arrivals.train_name, arrivals.type_code,
               arrivals.stop_number, arrivals.station_code, arrivals.station_name,
               arrivals.previous_station_code, arrivals.previous_station_name,
               arrivals.scheduled_arrival,
               arrivals.scheduled_arrival
                   + p.predicted_delay_minutes * INTERVAL '1 minute' AS expected_arrival,
               p.predicted_delay_minutes, p.prediction_source,
               p.priority_level,
               p.overtaking, TRUE AS live
        FROM arrivals
        JOIN train_positions p ON p.train_no = arrivals.train_no
            AND p.service_date = arrivals.origin_service_date
            AND p.event_timestamp >= NOW() - INTERVAL '15 seconds'
        ORDER BY arrivals.scheduled_arrival, arrivals.train_no
        LIMIT %s
        """,
        (station_code, window_start, window_end, limit),
    )
    return {
        "station_code": station_code,
        "window_start": window_start,
        "window_end": window_end,
        "service_day_assumption": "Daily service; the source timetable contains no weekday-of-operation fields.",
        "arrivals": rows,
    }


@app.get("/api/trains/{train_no}/route")
def train_route(train_no: str):
    try:
        normalized_train_no = str(int(train_no))
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Train number must be numeric") from error

    route_rows = query_rows(
        """
        SELECT t.train_no, t.train_name, t.type_code, r.stop_number, r.station_code,
               s.station_name, s.zone, s.latitude, s.longitude,
               r.distance_from_origin_km, r.arrival_day, r.arrival_time,
               r.departure_day, r.departure_time
        FROM trains t
        JOIN route_stops r ON r.train_no = t.train_no
        LEFT JOIN stations s ON s.station_code = r.station_code
        WHERE t.train_no = %s
        ORDER BY r.stop_number
        """,
        (normalized_train_no,),
    )
    if not route_rows:
        raise HTTPException(status_code=404, detail="Train route not found")
    position = query_rows(
        "SELECT * FROM train_positions WHERE train_no = %s",
        (normalized_train_no,),
    )
    current_position = position[0] if position else None
    service_date = (
        current_position["service_date"]
        if current_position
        else datetime.now(INDIA_TZ).date()
    )
    predicted_delay = (
        float(current_position["predicted_delay_minutes"])
        if current_position
        else 0.0
    )
    for stop in route_rows:
        scheduled_arrival = scheduled_stop_datetime(stop, "arrival", service_date)
        scheduled_departure = scheduled_stop_datetime(stop, "departure", service_date)
        stop["scheduled_arrival"] = scheduled_arrival
        stop["expected_arrival"] = expected_time_with_delay(
            scheduled_arrival, predicted_delay
        )
        stop["scheduled_departure"] = scheduled_departure
        stop["expected_departure"] = (
            scheduled_departure + timedelta(minutes=predicted_delay)
            if scheduled_departure
            else None
        )
        stop["simulated_actual_arrival"] = simulated_station_arrival(
            stop, route_rows[0], service_date, SIMULATION_SPEED
        )
        if current_position and stop["station_code"] == current_position["current_station_code"]:
            stop["status"] = "Current"
        elif current_position and stop["station_code"] == current_position["next_station_code"]:
            stop["status"] = "Next stop"
        elif current_position and stop["stop_number"] < current_position["route_index"]:
            stop["status"] = "Passed"
        else:
            stop["status"] = "Upcoming"
    return {
        "train": {
            "train_no": route_rows[0]["train_no"],
            "train_name": route_rows[0]["train_name"],
            "type_code": route_rows[0]["type_code"],
        },
        "position": current_position,
        "stops": route_rows,
    }


@app.get("/api/analytics/overview")
def analytics_overview():
    rows = query_rows(
        """
        SELECT
            COALESCE(SUM(observation_count), 0) AS historical_observations,
            COALESCE(
                SUM(average_delay_minutes * observation_count)
                / NULLIF(SUM(observation_count), 0),
                0
            ) AS weighted_average_delay_minutes,
            COALESCE(
                SUM(delayed_fraction * observation_count)
                / NULLIF(SUM(observation_count), 0),
                0
            ) AS weighted_delayed_fraction
        FROM train_delay_analytics
        """
    )
    return rows[0]


@app.get("/api/analytics/zones")
def zone_analytics(limit: int = Query(default=20, ge=1, le=100)):
    return query_rows(
        """
        SELECT
            COALESCE(NULLIF(zone, ''), 'Unknown') AS zone,
            COUNT(*) AS station_count,
            SUM(observation_count) AS observation_count,
            SUM(average_delay_minutes * observation_count)
                / NULLIF(SUM(observation_count), 0) AS average_delay_minutes,
            SUM(delayed_fraction * observation_count)
                / NULLIF(SUM(observation_count), 0) AS delayed_fraction,
            MAX(p90_delay_minutes) AS worst_station_p90_minutes
        FROM station_delay_analytics
        GROUP BY COALESCE(NULLIF(zone, ''), 'Unknown')
        ORDER BY average_delay_minutes DESC
        LIMIT %s
        """,
        (limit,),
    )


@app.get("/api/analytics/types")
def train_type_analytics():
    return query_rows(
        """
        SELECT
            COALESCE(NULLIF(type_code, ''), 'UNKNOWN') AS type_code,
            COUNT(*) AS train_count,
            SUM(observation_count) AS observation_count,
            SUM(average_delay_minutes * observation_count)
                / NULLIF(SUM(observation_count), 0) AS average_delay_minutes,
            SUM(delayed_fraction * observation_count)
                / NULLIF(SUM(observation_count), 0) AS delayed_fraction
        FROM train_delay_analytics
        GROUP BY COALESCE(NULLIF(type_code, ''), 'UNKNOWN')
        ORDER BY average_delay_minutes DESC
        """
    )


@app.get("/api/analytics/live-trends")
def live_analytics_trends(hours: int = Query(default=6, ge=1, le=24)):
    return query_rows(
        """
        SELECT
            date_bin(
                INTERVAL '15 minutes',
                event_timestamp,
                TIMESTAMPTZ '2000-01-01 00:00:00+00'
            ) AS time_bucket,
            COUNT(*) AS event_count,
            COUNT(DISTINCT train_no) AS trains_observed,
            ROUND(AVG(predicted_delay_minutes)::numeric, 1) AS average_delay_minutes,
            ROUND(
                (AVG(speed_kmph) FILTER (
                    WHERE speed_kmph BETWEEN 0 AND
                        CASE WHEN priority_level = 1 THEN 140 ELSE 100 END
                ))::numeric,
                1
            ) AS average_speed_kmph,
            COUNT(*) FILTER (
                WHERE speed_kmph >
                    CASE WHEN priority_level = 1 THEN 140 ELSE 100 END
            ) AS out_of_limit_speed_samples,
            COUNT(*) FILTER (WHERE predicted_delay_minutes > 15) AS delayed_events
        FROM train_events
        WHERE event_timestamp >= NOW() - (%s * INTERVAL '1 hour')
        GROUP BY time_bucket
        ORDER BY time_bucket
        """,
        (hours,),
    )

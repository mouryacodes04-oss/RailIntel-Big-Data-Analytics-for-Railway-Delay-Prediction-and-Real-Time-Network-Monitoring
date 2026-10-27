CREATE TABLE IF NOT EXISTS stations (
    station_code TEXT PRIMARY KEY,
    station_name TEXT NOT NULL,
    state TEXT,
    zone TEXT,
    address TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS trains (
    train_no TEXT PRIMARY KEY,
    train_name TEXT NOT NULL,
    type_code TEXT
);

CREATE TABLE IF NOT EXISTS route_stops (
    train_no TEXT NOT NULL REFERENCES trains(train_no) ON DELETE CASCADE,
    stop_number INTEGER NOT NULL,
    station_code TEXT NOT NULL,
    distance_from_origin_km DOUBLE PRECISION NOT NULL DEFAULT 0,
    arrival_day INTEGER,
    arrival_time TEXT,
    departure_day INTEGER,
    departure_time TEXT,
    PRIMARY KEY (train_no, stop_number)
);

CREATE INDEX IF NOT EXISTS route_stops_station_idx ON route_stops(station_code);

CREATE TABLE IF NOT EXISTS train_events (
    event_id UUID PRIMARY KEY,
    train_no TEXT NOT NULL REFERENCES trains(train_no),
    event_timestamp TIMESTAMPTZ NOT NULL,
    service_date DATE NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    speed_kmph DOUBLE PRECISION NOT NULL,
    current_station_code TEXT,
    next_station_code TEXT,
    route_index INTEGER NOT NULL,
    progress_pct DOUBLE PRECISION NOT NULL,
    distance_to_next_km DOUBLE PRECISION NOT NULL,
    predicted_delay_minutes DOUBLE PRECISION NOT NULL,
    predicted_arrival TIMESTAMPTZ NOT NULL,
    prediction_source TEXT NOT NULL,
    simulated BOOLEAN NOT NULL DEFAULT TRUE,
    priority_level INTEGER NOT NULL DEFAULT 2,
    overtaking BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS train_events_timestamp_idx ON train_events(event_timestamp DESC);
CREATE INDEX IF NOT EXISTS train_events_train_timestamp_idx ON train_events(train_no, event_timestamp DESC);

CREATE TABLE IF NOT EXISTS train_positions (
    train_no TEXT PRIMARY KEY REFERENCES trains(train_no),
    event_id UUID NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    service_date DATE NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    speed_kmph DOUBLE PRECISION NOT NULL,
    current_station_code TEXT,
    next_station_code TEXT,
    route_index INTEGER NOT NULL,
    progress_pct DOUBLE PRECISION NOT NULL,
    distance_to_next_km DOUBLE PRECISION NOT NULL,
    predicted_delay_minutes DOUBLE PRECISION NOT NULL,
    predicted_arrival TIMESTAMPTZ NOT NULL,
    prediction_source TEXT NOT NULL,
    simulated BOOLEAN NOT NULL DEFAULT TRUE,
    priority_level INTEGER NOT NULL DEFAULT 2,
    overtaking BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS station_delay_analytics (
    station_code TEXT PRIMARY KEY,
    station_name TEXT NOT NULL,
    zone TEXT,
    observation_count BIGINT NOT NULL,
    average_delay_minutes DOUBLE PRECISION NOT NULL,
    p90_delay_minutes DOUBLE PRECISION NOT NULL,
    delayed_fraction DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS train_delay_analytics (
    train_no TEXT PRIMARY KEY,
    train_name TEXT NOT NULL,
    type_code TEXT,
    observation_count BIGINT NOT NULL,
    average_delay_minutes DOUBLE PRECISION NOT NULL,
    p90_delay_minutes DOUBLE PRECISION NOT NULL,
    delayed_fraction DOUBLE PRECISION NOT NULL
);

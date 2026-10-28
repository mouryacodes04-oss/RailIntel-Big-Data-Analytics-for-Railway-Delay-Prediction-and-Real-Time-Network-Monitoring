# Railway Live Analytics

A local Big Data project for Indian railway operations analytics. It combines the supplied railway network and delay datasets with a Kafka-based train-position simulator, Spark MLlib delay training, a Hadoop HDFS lakehouse, PostgreSQL, and a live map dashboard.

Train locations and live statuses are **simulated** from the supplied route and timetable datasets. This project does not connect to live railway GPS or official train-status services.

## Architecture

```text
Railway_Network + Railway_delays datasets
      ├── catalog loader ────────────────> PostgreSQL route/station catalog
      └── Spark batch ETL ───────────────> Hadoop HDFS Parquet silver/gold + model sample
                                                ├── Spark MLlib delay model
                                                └── historical analytics summaries

Route simulator ──> Kafka (train_positions) ──> prediction consumer ──> PostgreSQL
                                                                           │
                                                               FastAPI + Leaflet dashboard
```

### Services

- **Kafka (KRaft):** keyed position events, partitioned by train number.
- **Schedule-aware simulator:** selects up to 200 trains whose supplied timetable has them in service at the current India-local time, advances the timetable at 4x simulation speed by default, and emits events every two seconds. While moving, synthetic speed readings smoothly accelerate and decelerate between randomized speed targets; readings stay within priority-based speed limits and are zero at scheduled stops. The source datasets do not contain live speed measurements.
- **Speed limits:** simulated high-priority trains are capped at 140 km/h; Express and standard trains are capped at 100 km/h. Train speed is zero while stopped at a scheduled station.
- **Stream processor:** consumes events as a Kafka consumer group, estimates delay and next-stop ETA, and stores current positions plus event history. For dashboard variety, every fifth numeric train number is assigned a clearly labeled synthetic on-time scenario with a changing 0–15 minute delay; other trains retain the historical model estimate plus a smooth simulated delay variation. Neither behavior is a live railway delay feed.
- **Hadoop HDFS:** provides persistent distributed storage for partitioned Silver/Gold Parquet data and the fitted Spark MLlib pipeline. The local Compose demo uses one NameNode and one DataNode with replication set to one.
- **Apache Spark:** distributes the 38-million-row historical delay cleanup, joins schedule/train metadata, creates Parquet datasets in HDFS, and exports a bounded training sample.
- **Spark MLlib:** trains and temporally evaluates a regularized LinearRegression model. The fitted pipeline is saved in HDFS; a compact coefficient/category JSON artifact lets the Python stream processor make predictions without bundling Spark into the application container.
- **PostgreSQL:** holds route/station metadata, latest train locations, stream history, and imported historical analytics.
- **FastAPI + Leaflet:** serves station-to-station timetables, station arrival boards, live train tracking, route overlays, ETAs, and historical delay analytics.

## Requirements

- Docker Desktop with Docker Compose
- The source datasets linked below, downloaded and extracted under `Datasets/`
- Enough free disk space for the source CSV (about 1 GB) and generated Parquet data

## Download the datasets

The datasets are intentionally not committed to this repository. Download them from Kaggle and place the supplied files in the matching folders:

- [Indian Railways Dataset](https://www.kaggle.com/datasets/sripaadsrinivasan/indian-railways-dataset) — network station, train, and schedule data under `Datasets/Railway_Network/`.
- [Indian Railways Delay Dataset](https://www.kaggle.com/datasets/rxydenxd/indian-railways-delay-dataset) — delay and timetable CSV files under `Datasets/Railway_delays/`.

The application expects these files:

```text
Datasets/
├── Railway_Network/
│   ├── stations.json
│   ├── trains.json
│   └── schedules.json
└── Railway_delays/
    ├── combined_delay.csv
    ├── combined_schedule.csv
    ├── station_full_names.csv
    └── train_details.csv
```

Kaggle may require an account and acceptance of the dataset terms. Check each dataset's license and attribution requirements before redistribution.

## Run the live demo

Start the services:

```bash
docker compose up --build -d
```

On first startup, Compose imports station and route metadata from `Datasets/Railway_Network/stations.json` and `Datasets/Railway_delays/combined_schedule.csv`. The producer checks the timetable in the `Asia/Kolkata` time zone and publishes current positions for up to 200 currently scheduled trains by default. The exact active count changes with the time of day and the available timetable entries.

Open:

- Trains between stations: <http://localhost:8000/journeys>
- Live station arrivals: <http://localhost:8000/station-arrivals>
- Live train status: <http://localhost:8000/live-status>
- Big Data analytics: <http://localhost:8000/analytics>
- Dashboard default: <http://localhost:8000> (opens live train status)
- API docs: <http://localhost:8000/docs>
- Spark master UI: <http://localhost:8080>
- Hadoop NameNode UI: <http://localhost:9870>

## Project presentation

- [20-slide Railway Big Data Analytics presentation](./Railway_Big_Data_Analytics.pptx)
- [Slide-by-slide presenter script and expected questions](./Railway_Big_Data_Analytics_Presenter_Script.md)

The dashboard separates its primary workflows into four pages linked in the top navigation:

- **Trains between stations:** choose an origin, destination, and date to find matching services from the supplied `combined_schedule.csv` and train catalog. Results show only the selected origin and destination with their departure/arrival times; intermediate stops are hidden.
- **Live station arrivals:** select a station to see only trains with a fresh simulated position that are due in the next four hours. The board shows the timetable leg as **From → To** and calculates expected arrival using the live prediction only; historical dataset averages are not shown. The window refreshes periodically and crosses midnight correctly.
- **Live Train Status:** view scheduled trains on the India map and train list. The selected-service number/name and full-name origin → destination appear inline, followed by current/next station details and expected arrival. The station-by-station table assigns a stable, train-specific randomized delay from 0–200 minutes to every stop, including upcoming stops; each expected arrival is its scheduled time plus that stop's simulated delay. Values do not change with live refreshes, and every route includes at least one zero-delay stop shown as **On time**. Delay and status occupy separate table columns, with each badge contained in its own cell so they cannot merge. Passed rows have a distinct background, and current/next rows use their own highlights. The table scrolls independently and remains usable on mobile. All displayed times use India Standard Time in date plus 24-hour `HH:mm` format. Selecting a train zooms to it once; later updates move the marker but do not recenter the map.
- **Big Data analytics:** the dedicated dashboard separates **Dataset Analytics** (Spark Gold summaries, observation-weighted historical delay KPIs, and railway-zone/train-type comparisons) from **Current Data Analytics** (recent Kafka event delay/speed trends, event throughput, and current fleet delay/priority distributions). Individual historical station/train delay rankings are not displayed. Historical records and simulated streaming values are not combined; out-of-limit legacy speed events are counted and excluded from average-speed trends.

Use the map layer control to switch to the rail-network-only map. Train markers are colored by predicted delay band.
All displayed timestamps across the dashboard use India Standard Time in a consistent date and 24-hour `HH:mm` format; seconds and AM/PM markers are omitted.

### Dashboard workflows

1. **Find a journey:** enter a station code or name in both station fields, choose a suggestion, select a date from the calendar or use **Today**/**Tomorrow**, then click **Find trains**. Each result shows the train, selected origin, selected destination, and their scheduled departure/arrival times; intermediate stops are not shown. Times are displayed in India Standard Time.
2. **Check a station:** enter a station code or name under **Live station arrivals**. The board filters to arrivals with a matching train position updated within the last 15 seconds and the same service date. It shows the previous scheduled stop → selected station, scheduled arrival, the live expected arrival, and live predicted delay. Historical dataset averages are not used; if there are no matching current trains, the page says so. Use **Refresh arrivals** or wait for the 30-second refresh.
3. **Track a train:** on **Live Train Status**, select a row or map marker. The map zooms to the train once and leaves the view in place afterward while the marker continues to update. The selected marker is purple; route stations are labeled and the next stop is highlighted. The compact details card keeps the selected service and full journey on one line when space permits, and shows the current station's simulated arrival timestamp and expected next-stop arrival. The arrival timestamp is derived from accelerated timetable playback, not real railway telemetry. The full-width panel includes a compact route timeline with full station names and hover tooltips. Its timetable has separate **Delay** and **Status** columns with distinct badge styling, a unique background for passed stops, and purple/amber highlights for current/next stops. Delay values are stable per stop and do not change during the two-second live refresh. Historical analytics refresh every 30 seconds.
4. **Review analytics:** open **Big Data Analytics** for historical zone and train-type comparisons and current simulated streaming trends. Individual historical station/train delay rankings are not shown. Historical delay KPIs are weighted from the Spark-published train aggregates, and the live trend window is selectable from 3 to 24 hours. Streaming trend buckets are built from recent simulated Kafka events; the two sources are intentionally not combined. The simulator assigns every fifth numeric train number a synthetic on-time delay between 0 and 15 minutes so the live fleet demonstrates both on-time and delayed services. Legacy event speeds above the current priority cap remain in event totals but are excluded from average-speed charts and surfaced as a data-quality count.

Useful checks:

```bash
docker compose ps
docker compose logs -f producer stream-processor api
curl http://localhost:8000/api/health
curl http://localhost:8000/api/trains
```

The dashboard labels predictions as **baseline** until a historical model is trained. Its baseline delay is 15 minutes and is deliberately identified as such; it is not presented as an ML result.

Adjust the local simulation in `.env` or on the command line:

```bash
ACTIVE_TRAINS=300 EVENT_INTERVAL_SECONDS=3 docker compose up -d
```

`ACTIVE_TRAINS` is a maximum, not a guaranteed count: only trains whose timetable puts them in service at the current India-local time are emitted. `EVENT_INTERVAL_SECONDS` defaults to two seconds and `SIMULATION_SPEED` defaults to 4. Positions follow timetable-derived progress at the accelerated playback rate; speed values and changing delay variation are synthetic, not sensor measurements.

## Run historical Big Data analytics and train the ML model

The historical delay file is large, so this full Spark job is run explicitly rather than on every demo startup. It cleans and enriches delay observations, writes partitioned Parquet to Hadoop HDFS, and creates train/station Gold summaries and a bounded model-training sample:

```bash
docker compose run --rm analytics bash -lc \
  '$SPARK_HOME/bin/spark-submit --master spark://spark-master:7077 --executor-memory 512m --conf spark.cores.max=1 /app/jobs/prepare_data.py'
```

The ETL filters null/malformed keys and delays outside `-120` to `1440` minutes, joins schedule stop sequence and distance, and writes the cleaned outputs under `hdfs://namenode:9000/railway/lake/`. The local quality report is under `data/lake/`; the HDFS NameNode and DataNode contents persist in Docker volumes.

Train and evaluate the delay model using Spark MLlib and the Spark-produced HDFS sample:

```bash
docker compose run --rm analytics bash -lc \
  '$SPARK_HOME/bin/spark-submit --master spark://spark-master:7077 --executor-memory 512m --conf spark.cores.max=1 /app/jobs/train_model.py'
```

The trainer uses a time-ordered 80/20 split, reports MAE/RMSE, writes the fitted Spark pipeline to `hdfs://namenode:9000/railway/lake/models/delay_pipeline`, and exports `models/delay_model.json` for low-latency Python inference. Training metrics are written to `models/metrics.json`.

The verified full-sample run used 183,163 rows (146,006 train / 37,157 temporal test rows; cutoff `2025-11-26`) and reported MAE `32.08` minutes and RMSE `65.65` minutes. These are dataset-split metrics, not a production accuracy claim.

Publish historical station and train summaries from HDFS to PostgreSQL:

```bash
docker compose run --rm analytics bash -lc \
  '$SPARK_HOME/bin/spark-submit --master spark://spark-master:7077 --executor-memory 512m --conf spark.cores.max=1 /app/jobs/publish_analytics.py'
```

Check the lakehouse from the NameNode container with `docker compose exec hdfs-namenode hdfs dfs -ls -R /railway/lake`.

Restart the consumer so it loads the newly trained model:

```bash
docker compose restart stream-processor
```

Generated model artifacts are git-ignored. After the consumer restarts, the dashboard reports Spark MLlib-backed historical predictions. Individual historical station/train delay rankings are not displayed. If no JSON model is available, the stream processor explicitly labels predictions as baseline; older `.joblib` models remain supported as a migration fallback.

## Data used

| Dataset | Use |
|---|---|
| `Railway_Network/stations.json` | Station names, zones, states, addresses, and coordinates |
| `Railway_Network/trains.json` | Train route metadata (available for extensions) |
| `Railway_Network/schedules.json` | Network schedule reference (available for extensions) |
| `Railway_delays/combined_schedule.csv` | Ordered station stops and distances used to build routes |
| `Railway_delays/train_details.csv` | Train names and types |
| `Railway_delays/combined_delay.csv` | Historical delay labels and analytics (38.4 million rows) |
| `Railway_delays/station_full_names.csv` | Station code/name/zone enrichment for reports |

Station coordinates are incomplete in the supplied network metadata, so the map simulator only activates routes with at least two geocoded stops. Route order comes from the schedule; **the next station is a schedule lookup, not an ML classification**. The ML model estimates delay at that stop, and that estimate contributes to the expected arrival time.

The supplied timetable gives each stop's arrival/departure time and a trip-day offset (for example, a train's second service day); it does **not** include dates or weekdays on which each train runs. Consequently, the simulator and timetable search assume listed services operate daily. The active count is the intersection of the current timetable window and the `ACTIVE_TRAINS` cap, not a random fleet size. Live station boards include only fresh train positions matching the timetable service date and use the model's next-stop prediction or current live delay estimate; historical dataset averages are intentionally excluded from this live view.

Priority levels and overtaking are illustrative simulation fields only. They do not model real railway signaling, track capacity, or operational authority to overtake. The map joins station coordinates with straight lines; it is not official track geometry.

For the focused automated tests, run them in the application container:

```bash
docker compose run --rm -v "$PWD/tests:/tests:ro" producer python -m unittest discover -s /tests -v
```

## API overview

- `GET /api/health` — service/database readiness
- `GET /api/stats` — active train and prediction summary
- `GET /api/trains` — latest position, next stop, ETA, and delay estimate
- `GET /api/stations?q=...` — station catalog search for route and station selectors
- `GET /api/trains/between?from_station=NDLS&to_station=BCT` — matching services and intermediate timetable
- `GET /api/stations/{station_code}/arrivals` — scheduled and expected arrivals for a station
- `GET /api/trains/{train_no}/route` — ordered route with scheduled/expected times and station coordinates
- `GET /api/analytics/overview` — observation-weighted historical delay summary
- `GET /api/analytics/zones` — station delay rollups by railway zone
- `GET /api/analytics/types` — train delay rollups by train type
- `GET /api/analytics/live-trends?hours=6` — 15-minute rollups of recent stream events (1–24 hours)

## Operational notes

- This is a local educational/demo stack. The sample database credentials are not for production.
- The historical Spark job processes a roughly 1 GB CSV and can take substantial time and disk space. The single-NameNode/single-DataNode Compose HDFS setup is for local learning, not highly available production storage.
- Schedule-based service-day filtering is performed in India Standard Time. Dataset day offsets describe a train's itinerary, not weekly operating days; daily operation is an explicit demo assumption until a run-days source is provided.
- The current ML target is observed delay in minutes at a scheduled stop. The model uses train number, route stop sequence, distance from origin, calendar features, and train type; it is not trained on live GPS telemetry.
- Delay values include early arrivals and extreme outliers. The ETL preserves early values down to -120 minutes and excludes extreme values above 1440 minutes.
- For reliability testing, scale the consumer with `docker compose up --scale stream-processor=2 -d`; Kafka partitions by train number so a train's events stay ordered within one partition.

## Stop services

```bash
docker compose down
```

To also delete persisted Kafka and PostgreSQL data, explicitly remove the named volumes with `docker compose down -v`.

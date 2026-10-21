# ltc-visualizer

Collects London Transit Commission GTFS-realtime data into MongoDB for a
dashboard (speed, schedule deviation, active vehicles, bunching,
on-time/early/late %).

## Run

```bash
docker compose up -d                 # MongoDB on 127.0.0.1:27017 (no auth, localhost only)
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python collector/poller.py # polls every 60s; Ctrl-C to stop
```

## What gets stored

One document per poll in `ltc.snapshots`:

| field | type   | notes                                                    |
|-------|--------|----------------------------------------------------------|
| `ts`  | Date   | TripUpdates feed timestamp; unique index **with TTL**    |
| `n`   | int    | vehicles assigned to a trip (see notes below)            |
| `tu`  | Binary | trimmed TripUpdates protobuf: current trip per vehicle, next stop's arrival and departure delay/time only |
| `vp`  | Binary | trimmed VehiclePositions protobuf: lat/lon/bearing/speed per vehicle |

Decode the blobs with `db.decode_snapshot(snapshot)` (in `collector/`).

Notes for building metrics from `tu`:

- **Missing delays are left unset, not stored as 0.** LTC omits the delay for
  some next stops (~9% of vehicles in a sample), so check
  `HasField("delay")` before using it and treat the rest as unknown. Reading
  `.delay` directly returns 0 for those. Snapshots stored before this fix
  have them as 0 and can't be told apart.
- **Use the departure delay for on-time %.** At timing points a bus can
  arrive late but leave on schedule (e.g. +452 s arrival vs +92 s departure),
  so arrival delay overstates lateness. Snapshots stored before this change
  have only the arrival.
- **`tu` / `n` include buses waiting to start a trip** (e.g. at a terminal
  between trips), not just buses mid-route: ~13% in a sample. Exclude them for
  bunching with `static_gtfs.is_before_first_stop(trip, stop_sequence)`.
- **Direction and route shape come from the static GTFS** (below): LTC's
  realtime feed only has `direction_id` for about a third of trips.

A raw TripUpdates response is ~3.1 MB. After trimming, a poll is ~27 KB
(~17 KB TU + ~10 KB VP) before Mongo's zstd compression, or roughly **1 GB per
month**. VehiclePositions is also polled because TripUpdates has no
location or speed, which you need for average speed and bunching. Set
`STORE_VEHICLE_POSITIONS=0` to skip it.

## Polling and retries

- Each poll fetches TripUpdates with `If-None-Match`, so an unchanged feed
  (`304`) is skipped, and a snapshot whose feed timestamp is already stored is
  skipped too. One request per feed at a time; no catch-up bursts after a
  delay.
- **LTC request failures** (rate limiting, 5xx, timeouts, network) back off
  exponentially: `POLL_INTERVAL_S`, then doubling up to 10 minutes. A
  `Retry-After` header is honoured (up to 1 hour). The normal interval resumes
  after the next successful poll.
- **MongoDB** is retried with backoff (1 s up to 60 s) at startup; after that,
  a failed insert is retried on the next poll.

## Static GTFS

The poller also checks LTC's static GTFS zip once a day (an hour after a
failed check), with `If-None-Match` / `If-Modified-Since` so an unchanged file
isn't downloaded. It's stored in the same database (`collector/static_gtfs.py`
documents the collections):

- **Versions are identified by their contents**: a hash of every file in the
  zip, not `feed_version` or the ETag. Any changed file is a new version; a
  re-uploaded identical zip isn't.
- **Each version is kept** (its original zip, plus trips, shapes, stops and
  routes for lookups) until its end date is more than `RETENTION_DAYS` ago,
  so every retained snapshot can still be matched. The version LTC currently
  serves is never deleted.
- **Pick the version by the trip's service date**, not "latest": LTC
  publishes versions a few days before they take effect.

Lookups (each takes the Mongo `Database`):

| function | returns |
|----------|---------|
| `resolve_trips(database, service_date, trip_ids)` | static info for realtime trips (direction, shape, block, first/last stop sequence, `version`), using the version in effect on `service_date` (the TripUpdates `start_date`) |
| `is_before_first_stop(trip, stop_sequence)` | whether a bus hasn't started its trip yet |
| `version_for_date` / `versions_for_date` / `latest_version` | version ids / records |
| `get_trips` / `get_shapes` / `get_stops` / `get_routes(database, version, ids=None)` | id -> record for one version; shapes include cumulative `dist_m` along the route |
| `read_table(database, version, "stop_times.txt")` | rows of any file in that version's zip |

## Configuration (env vars)

| var | default |
|-----|---------|
| `MONGO_URI` | `mongodb://localhost:27017` |
| `MONGO_DB` / `MONGO_COLLECTION` | `ltc` / `snapshots` |
| `POLL_INTERVAL_S` | `60` |
| `RETENTION_DAYS` | `35`. **TODO:** raise once real storage numbers are in; restarting the poller updates the TTL in place |
| `STORAGE_BUDGET_GB` | `12`, used only for the hourly storage projection in the logs |
| `STORE_VEHICLE_POSITIONS` | `1` |
| `STATIC_GTFS_URL` | `https://www.londontransit.ca/gtfsfeed/google_transit.zip` |

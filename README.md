# ltc-visualizer

Collects London Transit Commission GTFS-realtime data into MongoDB for a
dashboard (speed, schedule deviation, active vehicles, bunching,
on-time/early/late %).

## Run

```bash
docker compose up -d            # MongoDB on 127.0.0.1:27017 (no auth, localhost only) + poller
                                # + backend (Express) on 127.0.0.1:3000; GET /health pings Mongo
docker compose up -d --build    # rebuild after code changes
docker compose logs -f poller   # follow poller logs
docker compose down             # stop (Mongo data is kept in the volume)
```

## What gets stored

Each realtime feed has its own collection, with one document per new version
of the feed:

| collection | field | type | notes |
|------------|-------|------|-------|
| both | `ts` | Date | that feed's timestamp; unique index **with TTL** |
| `ltc.snapshots` | `n` | int | vehicles assigned to a trip (see notes below) |
| | `tu` | Binary | trimmed TripUpdates protobuf: current trip per vehicle, next stop's arrival and departure delay/time only |
| `ltc.vehicle_positions` | `n` | int | vehicles with a position |
| | `vp` | Binary | trimmed VehiclePositions protobuf: lat/lon/bearing/speed per vehicle |

Snapshots stored before the feeds were polled separately have both `tu` and
`vp` in `ltc.snapshots`. Decode any of them with `db.decode_snapshot(snapshot)`
(in `collector/`), which returns `(trip_updates, vehicle_positions)` with
`None` for a feed the document doesn't hold. To line up positions with trip
updates, match on the nearest `ts`.

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
- **The stored next stop comes from TripUpdates' own predictions**, not
  VehiclePositions' `current_stop_sequence`, which LTC often reports several
  stops ahead of the bus (sometimes past the trip's last stop) when it doesn't
  also send `current_status`. Snapshots stored before this fix put ~30% of
  buses at a stop minutes ahead of them (a median of 500 m from the stored
  stop instead of ~180 m in a sample).

A raw TripUpdates response is ~3.1 MB. After trimming, a poll is ~27 KB
(~17 KB TU + ~10 KB VP) before Mongo's zstd compression, or roughly **1 GB per
month** at the default intervals. VehiclePositions is also polled because TripUpdates has no
location or speed, which you need for average speed and bunching. Set
`STORE_VEHICLE_POSITIONS=0` to skip it.

## Backend metrics

`GET /metrics/<metric>` covers all routes; `GET /metrics/<metric>/<route>`
covers one route (`2` and `02` both match). By default the metrics come from
the latest snapshot. Add `?from=<ISO date>&to=<ISO date>` to pool every
snapshot in that range. Snapshots are decoded in `backend/metrics.js`, since
Mongo can't aggregate the protobuf fields.

| metric | returns |
|--------|---------|
| `speed` | `avgSpeedKmh` over vehicles that report a speed (~60%) |
| `deviation` | `avgDelaySec` (+ = late) and `avgAbsDelaySec`; departure delay, falling back to arrival; unknown delays skipped |
| `punctuality` | `earlyPct` / `onTimePct` / `latePct`; on time = 1 min early to 5 min late |
| `vehicles` | `active` and `bunched`, averaged per snapshot. Bunched = within 250 m (straight line) of another bus on the same route and direction, both past their first stop |

Bunching takes direction and first stop from the static GTFS, falling back to
the realtime `direction_id` when the trip isn't found there.

## Polling and retries

- **Each feed is polled independently**: TripUpdates, VehiclePositions and
  the static GTFS each run in their own thread with their own schedule, HTTP
  session, de-duplication and backoff. A slow or failing feed doesn't delay or
  stop the others.
- TripUpdates uses the latest VehiclePositions trip assignments to pick each
  bus's current trip, if they're under `max(120 s, 2 × the VehiclePositions
  interval)` old; otherwise it picks from its own data. It never waits for
  VehiclePositions.
- Each request sends `If-None-Match`, so an unchanged feed (`304`) is
  skipped, and a version whose feed timestamp is already stored is skipped
  too. One request per feed at a time; no catch-up bursts after a delay.
- LTC currently publishes both realtime feeds every 30 s, so polling faster
  than that only produces `304`s.
- **LTC request failures** (rate limiting, 5xx, timeouts, network) back off
  exponentially per feed: that feed's interval, then doubling up to 10
  minutes. A `Retry-After` header is honoured (up to 1 hour). The normal
  interval resumes after the next successful poll.
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
| `MONGO_URI` | `mongodb://mongo:27017` (set in `docker-compose.yml`) |
| `MONGO_DB` | `ltc` |
| `TRIP_UPDATES_COLLECTION` | `snapshots` (`MONGO_COLLECTION` still works as the old name) |
| `VEHICLE_POSITIONS_COLLECTION` | `vehicle_positions` |
| `POLL_INTERVAL_S` | `60`, the default for both realtime feeds |
| `TRIP_UPDATES_POLL_INTERVAL_S` / `VEHICLE_POSITIONS_POLL_INTERVAL_S` | `POLL_INTERVAL_S` |
| `RETENTION_DAYS` | `35`. **TODO:** raise once real storage numbers are in; restarting the poller updates the TTL in place |
| `STORAGE_BUDGET_GB` | `12`, used only for the hourly storage projection in the logs |
| `STORE_VEHICLE_POSITIONS` | `1` |
| `STATIC_GTFS_URL` | `https://www.londontransit.ca/gtfsfeed/google_transit.zip` |

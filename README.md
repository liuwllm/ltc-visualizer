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
| `tu`  | Binary | trimmed TripUpdates protobuf: current trip per vehicle, next-stop delay only |
| `vp`  | Binary | trimmed VehiclePositions protobuf: lat/lon/bearing/speed per vehicle |

Decode the blobs with `db.decode_snapshot(snapshot)` (in `collector/`).

Notes for building metrics from `tu`:

- **Missing delays are left unset, not stored as 0.** LTC omits the delay for
  some next stops (~9% of vehicles in a sample), so check
  `HasField("delay")` before using it and treat the rest as unknown. Reading
  `.delay` directly returns 0 for those. Snapshots stored before this fix
  have them as 0 and can't be told apart.
- **`tu` / `n` include buses waiting to start a trip** (e.g. at a terminal
  between trips), not just buses mid-route: ~13% in a sample. Exclude them for
  bunching; they're the ones whose next stop is the trip's first stop.

A raw TripUpdates response is ~3.1 MB. After trimming, a poll is ~25 KB
(~14 KB TU + ~10 KB VP) before Mongo's zstd compression, or roughly **1 GB per
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

## Configuration (env vars)

| var | default |
|-----|---------|
| `MONGO_URI` | `mongodb://localhost:27017` |
| `MONGO_DB` / `MONGO_COLLECTION` | `ltc` / `snapshots` |
| `POLL_INTERVAL_S` | `60` |
| `RETENTION_DAYS` | `35`. **TODO:** raise once real storage numbers are in; restarting the poller updates the TTL in place |
| `STORAGE_BUDGET_GB` | `12`, used only for the hourly storage projection in the logs |
| `STORE_VEHICLE_POSITIONS` | `1` |

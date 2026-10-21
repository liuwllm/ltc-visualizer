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
| `n`   | int    | active vehicles                                          |
| `tu`  | Binary | trimmed TripUpdates protobuf: current trip per vehicle, next-stop delay only |
| `vp`  | Binary | trimmed VehiclePositions protobuf: lat/lon/bearing/speed per vehicle |

Decode the blobs with `db.decode_snapshot(doc)` (in `collector/`).

A raw TripUpdates response is ~3.1 MB. After trimming, a poll is ~25 KB
(~14 KB TU + ~10 KB VP) before Mongo's zstd compression, or roughly **1 GB per
month**. VehiclePositions is also polled because TripUpdates has no
location or speed, which you need for average speed and bunching. Set
`STORE_VEHICLE_POSITIONS=0` to skip it.

## Configuration (env vars)

| var | default |
|-----|---------|
| `MONGO_URI` | `mongodb://localhost:27017` |
| `MONGO_DB` / `MONGO_COLLECTION` | `ltc` / `snapshots` |
| `POLL_INTERVAL_S` | `60` |
| `RETENTION_DAYS` | `35`. **TODO:** raise once real storage numbers are in; restarting the poller updates the TTL in place |
| `STORAGE_BUDGET_GB` | `12`, used only for the hourly storage projection in the logs |
| `STORE_VEHICLE_POSITIONS` | `1` |

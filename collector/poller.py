"""Poll LTC's GTFS-realtime feeds once a minute and store a trimmed copy in MongoDB.

Trimming lives in feeds.py and storage in db.py; this module fetches the feeds
and runs the loop. Set STORE_VEHICLE_POSITIONS=0 to skip VehiclePositions.

Run:  python collector/poller.py
"""

from __future__ import annotations

import email.utils
import logging
import os
import signal
import time
from datetime import datetime, timezone

import requests
from google.transit import gtfs_realtime_pb2 as gtfs
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

import db
import feeds

TRIP_UPDATES_URL = os.getenv(
    "TRIP_UPDATES_URL", "http://gtfs.ltconline.ca/TripUpdate/TripUpdates.pb"
)
VEHICLE_POSITIONS_URL = os.getenv(
    "VEHICLE_POSITIONS_URL", "http://gtfs.ltconline.ca/Vehicle/VehiclePositions.pb"
)
STORE_VEHICLE_POSITIONS = os.getenv("STORE_VEHICLE_POSITIONS", "1") == "1"

POLL_INTERVAL_S = int(os.getenv("POLL_INTERVAL_S", "60"))
HTTP_TIMEOUT_S = 20
# Backoff after consecutive failed LTC requests (rate limiting, outages):
# POLL_INTERVAL_S, then doubling up to this cap. A Retry-After header is honoured
# up to MAX_RETRY_AFTER_S.
MAX_BACKOFF_S = 600
MAX_RETRY_AFTER_S = 3600
STATS_INTERVAL_S = 3600

log = logging.getLogger("ltc-poller")


# --------------------------------------------------------------------------- #
# Polling
# --------------------------------------------------------------------------- #

class Poller:
    def __init__(self, coll: Collection):
        self.coll = coll
        self.http = requests.Session()
        self.tu_etag: str | None = None
        self.last_ts: datetime | None = db.latest_ts(coll)

    def _get(self, url: str, etag: str | None = None) -> requests.Response:
        headers = {"If-None-Match": etag} if etag else {}
        resp = self.http.get(url, headers=headers, timeout=HTTP_TIMEOUT_S)
        if resp.status_code != 304:
            resp.raise_for_status()
        return resp

    def _fetch_vehicle_positions(self) -> gtfs.FeedMessage | None:
        if not STORE_VEHICLE_POSITIONS:
            return None
        try:
            return feeds.trim_vehicle_positions(self._get(VEHICLE_POSITIONS_URL).content)
        except Exception as e:
            # Still worth storing the trip updates on their own.
            log.warning("VehiclePositions fetch failed: %s", e)
            return None

    def _is_stored(self, ts: datetime) -> bool:
        return self.last_ts is not None and ts <= self.last_ts

    def poll_once(self) -> None:
        resp = self._get(TRIP_UPDATES_URL, self.tu_etag)
        if resp.status_code == 304:
            log.info("TripUpdates unchanged since last poll; skipping")
            return
        # Only remember the ETag once the snapshot is safely stored, so a
        # failed insert (e.g. Mongo restarting) is retried next poll, not 304'd.
        etag = resp.headers.get("ETag")

        vp = self._fetch_vehicle_positions()
        trips = feeds.vehicle_trips(vp) if vp is not None else None
        tu, n_active = feeds.trim_trip_updates(resp.content, trips)

        ts = datetime.fromtimestamp(tu.header.timestamp, tz=timezone.utc)
        if self._is_stored(ts):
            self.tu_etag = etag
            log.info("Feed timestamp %s already stored; skipping", ts.isoformat())
            return

        doc = db.make_snapshot(ts, n_active, tu, vp)
        if not db.insert_snapshot(self.coll, doc):
            log.info("Snapshot %s already stored", ts.isoformat())
        self.tu_etag = etag
        self.last_ts = ts
        log.info(
            "stored %s: %d vehicles, tu=%d B (raw %d B), vp=%s B",
            ts.isoformat(), n_active, len(doc["tu"]), len(resp.content),
            len(doc["vp"]) if "vp" in doc else "-",
        )


# --------------------------------------------------------------------------- #
# Main loop
# --------------------------------------------------------------------------- #

class Shutdown:
    """Tracks SIGINT/SIGTERM and offers a sleep that wakes up on them."""

    def __init__(self):
        self.stopping = False
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def _stop(self, *_) -> None:
        self.stopping = True
        log.info("Shutting down after current poll")

    def sleep(self, seconds: float) -> None:
        # Sleep in short steps so Ctrl-C / SIGTERM is handled promptly.
        deadline = time.monotonic() + seconds
        while not self.stopping and time.monotonic() < deadline:
            time.sleep(min(1.0, deadline - time.monotonic()))


def connect_with_retry(client, shutdown: Shutdown) -> Poller | None:
    """Set up the collection and poller, retrying with backoff.

    Returns None if shut down before MongoDB became reachable.

    pymongo reconnects on its own once connected; this only covers startup,
    e.g. Mongo still coming up or temporarily down.
    """
    backoff = 1
    while True:
        try:
            return Poller(db.open_collection(client))
        except PyMongoError as e:
            log.warning("MongoDB unavailable (%s); retrying in %ds", e, backoff)
            shutdown.sleep(backoff)
            if shutdown.stopping:
                return None
            backoff = min(backoff * 2, 60)


def retry_after_s(e: requests.RequestException) -> float | None:
    """Seconds to wait from the response's Retry-After header, if any."""
    value = e.response.headers.get("Retry-After") if e.response is not None else None
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:  # HTTP-date form
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def backoff_s(failures: int, e: requests.RequestException) -> float:
    delay = min(POLL_INTERVAL_S * 2 ** (failures - 1), MAX_BACKOFF_S)
    retry_after = retry_after_s(e)
    if retry_after is not None:
        delay = max(delay, min(retry_after, MAX_RETRY_AFTER_S))
    return delay


def poll_safely(poller: Poller) -> requests.RequestException | None:
    """Poll once; returns the error if the LTC request failed, else None."""
    try:
        poller.poll_once()
    except requests.RequestException as e:  # rate limited, LTC down, network
        return e
    except PyMongoError as e:  # Mongo restarting; pymongo reconnects
        log.warning("Poll failed, MongoDB unavailable: %s", e)
    except Exception:  # keep polling through feed hiccups
        log.exception("Poll failed")
    return None


def log_storage_safely(coll: Collection) -> None:
    try:
        db.log_storage(coll, POLL_INTERVAL_S)
    except Exception:
        log.exception("Storage stats failed")


def run(poller: Poller, shutdown: Shutdown) -> None:
    next_tick = time.monotonic()
    next_stats = time.monotonic()
    failures = 0
    while not shutdown.stopping:
        err = poll_safely(poller)
        if time.monotonic() >= next_stats:
            log_storage_safely(poller.coll)
            next_stats += STATS_INTERVAL_S

        if err is not None:
            failures += 1
            delay = backoff_s(failures, err)
            log.warning(
                "LTC feed request failed (%d in a row): %s; retrying in %ds",
                failures, err, delay,
            )
            shutdown.sleep(delay)
            next_tick = time.monotonic()  # resume the normal schedule from here
            continue
        if failures:
            log.info("LTC feed recovered after %d failed polls", failures)
            failures = 0

        next_tick += POLL_INTERVAL_S
        shutdown.sleep(next_tick - time.monotonic())
        if time.monotonic() - next_tick > POLL_INTERVAL_S:
            next_tick = time.monotonic()  # fell far behind; don't burst-poll


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    shutdown = Shutdown()
    client = db.connect()
    try:
        poller = connect_with_retry(client, shutdown)
        if poller is None:
            return
        log.info("Connected to MongoDB")
        log.info(
            "Polling every %ds into %s.%s (retention %d days, vehicle positions %s)",
            POLL_INTERVAL_S, db.MONGO_DB, db.MONGO_COLLECTION, db.RETENTION_DAYS,
            "on" if STORE_VEHICLE_POSITIONS else "off",
        )
        run(poller, shutdown)
    finally:
        client.close()


if __name__ == "__main__":
    main()

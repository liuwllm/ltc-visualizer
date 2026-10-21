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
    def __init__(self, collection: Collection):
        self.collection = collection
        self.session = requests.Session()
        self.trip_updates_etag: str | None = None
        self.last_stored_time: datetime | None = db.latest_ts(collection)

    def _get(self, url: str, etag: str | None = None) -> requests.Response:
        headers = {"If-None-Match": etag} if etag else {}
        response = self.session.get(url, headers=headers, timeout=HTTP_TIMEOUT_S)
        if response.status_code != 304:
            response.raise_for_status()
        return response

    def _fetch_vehicle_positions(self) -> gtfs.FeedMessage | None:
        if not STORE_VEHICLE_POSITIONS:
            return None
        try:
            return feeds.trim_vehicle_positions(self._get(VEHICLE_POSITIONS_URL).content)
        except Exception as error:
            # Still worth storing the trip updates on their own.
            log.warning("VehiclePositions fetch failed: %s", error)
            return None

    def _is_stored(self, feed_time: datetime) -> bool:
        return self.last_stored_time is not None and feed_time <= self.last_stored_time

    def poll_once(self) -> None:
        response = self._get(TRIP_UPDATES_URL, self.trip_updates_etag)
        if response.status_code == 304:
            log.info("TripUpdates unchanged since last poll; skipping")
            return
        # Only remember the ETag once the snapshot is safely stored, so a
        # failed insert (e.g. Mongo restarting) is retried next poll, not 304'd.
        etag = response.headers.get("ETag")

        vehicle_positions = self._fetch_vehicle_positions()
        vehicle_trips = (
            feeds.vehicle_trips(vehicle_positions)
            if vehicle_positions is not None
            else None
        )
        trip_updates, active_vehicle_count = feeds.trim_trip_updates(
            response.content, vehicle_trips
        )

        feed_time = datetime.fromtimestamp(trip_updates.header.timestamp, tz=timezone.utc)
        if self._is_stored(feed_time):
            self.trip_updates_etag = etag
            log.info("Feed timestamp %s already stored; skipping", feed_time.isoformat())
            return

        snapshot = db.make_snapshot(
            feed_time, active_vehicle_count, trip_updates, vehicle_positions
        )
        if not db.insert_snapshot(self.collection, snapshot):
            log.info("Snapshot %s already stored", feed_time.isoformat())
        self.trip_updates_etag = etag
        self.last_stored_time = feed_time
        log.info(
            "stored %s: %d vehicles, tu=%d B (raw %d B), vp=%s B",
            feed_time.isoformat(), active_vehicle_count, len(snapshot["tu"]),
            len(response.content), len(snapshot["vp"]) if "vp" in snapshot else "-",
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
    retry_delay_s = 1
    while True:
        try:
            return Poller(db.open_collection(client))
        except PyMongoError as error:
            log.warning("MongoDB unavailable (%s); retrying in %ds", error, retry_delay_s)
            shutdown.sleep(retry_delay_s)
            if shutdown.stopping:
                return None
            retry_delay_s = min(retry_delay_s * 2, 60)


def retry_after_s(error: requests.RequestException) -> float | None:
    """Seconds to wait from the response's Retry-After header, if any."""
    header = (
        error.response.headers.get("Retry-After") if error.response is not None else None
    )
    if header is None:
        return None
    try:
        return max(0.0, float(header))
    except ValueError:
        pass
    try:  # HTTP-date form
        retry_at = email.utils.parsedate_to_datetime(header)
    except (TypeError, ValueError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())


def backoff_s(consecutive_failures: int, error: requests.RequestException) -> float:
    delay_s = min(POLL_INTERVAL_S * 2 ** (consecutive_failures - 1), MAX_BACKOFF_S)
    requested_delay_s = retry_after_s(error)
    if requested_delay_s is not None:
        delay_s = max(delay_s, min(requested_delay_s, MAX_RETRY_AFTER_S))
    return delay_s


def poll_safely(poller: Poller) -> requests.RequestException | None:
    """Poll once; returns the error if the LTC request failed, else None."""
    try:
        poller.poll_once()
    except requests.RequestException as error:  # rate limited, LTC down, network
        return error
    except PyMongoError as error:  # Mongo restarting; pymongo reconnects
        log.warning("Poll failed, MongoDB unavailable: %s", error)
    except Exception:  # keep polling through feed hiccups
        log.exception("Poll failed")
    return None


def log_storage_safely(collection: Collection) -> None:
    try:
        db.log_storage(collection, POLL_INTERVAL_S)
    except Exception:
        log.exception("Storage stats failed")


def run(poller: Poller, shutdown: Shutdown) -> None:
    next_poll_at = time.monotonic()
    next_stats_at = time.monotonic()
    consecutive_failures = 0
    while not shutdown.stopping:
        request_error = poll_safely(poller)
        if time.monotonic() >= next_stats_at:
            log_storage_safely(poller.collection)
            next_stats_at += STATS_INTERVAL_S

        if request_error is not None:
            consecutive_failures += 1
            delay_s = backoff_s(consecutive_failures, request_error)
            log.warning(
                "LTC feed request failed (%d in a row): %s; retrying in %ds",
                consecutive_failures, request_error, delay_s,
            )
            shutdown.sleep(delay_s)
            next_poll_at = time.monotonic()  # resume the normal schedule from here
            continue
        if consecutive_failures:
            log.info("LTC feed recovered after %d failed polls", consecutive_failures)
            consecutive_failures = 0

        next_poll_at += POLL_INTERVAL_S
        shutdown.sleep(next_poll_at - time.monotonic())
        if time.monotonic() - next_poll_at > POLL_INTERVAL_S:
            next_poll_at = time.monotonic()  # fell far behind; don't burst-poll


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

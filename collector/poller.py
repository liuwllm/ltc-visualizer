"""Poll LTC's feeds and store trimmed copies in MongoDB.

Each feed is its own job, in its own thread, with its own HTTP session,
schedule, ETag, de-duplication and backoff, so a slow or failing feed never
holds up the others:

* TripUpdates       every TRIP_UPDATES_POLL_INTERVAL_S      -> trip updates collection
* VehiclePositions  every VEHICLE_POSITIONS_POLL_INTERVAL_S -> vehicle positions collection
* static GTFS       daily (static_gtfs.py)

Trimming TripUpdates uses the most recent VehiclePositions trip assignments if
they're recent enough; it never waits for them.

Trimming lives in feeds.py and storage in db.py. Set STORE_VEHICLE_POSITIONS=0
to skip VehiclePositions.

Run:  python collector/poller.py
"""

from __future__ import annotations

import email.utils
import logging
import os
import signal
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone

import requests
from google.transit import gtfs_realtime_pb2 as gtfs
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError

import db
import feeds
import static_gtfs

TRIP_UPDATES_URL = os.getenv(
    "TRIP_UPDATES_URL", "http://gtfs.ltconline.ca/TripUpdate/TripUpdates.pb"
)
VEHICLE_POSITIONS_URL = os.getenv(
    "VEHICLE_POSITIONS_URL", "http://gtfs.ltconline.ca/Vehicle/VehiclePositions.pb"
)
STORE_VEHICLE_POSITIONS = os.getenv("STORE_VEHICLE_POSITIONS", "1") == "1"

# Default interval for both realtime feeds; each can be overridden.
POLL_INTERVAL_S = int(os.getenv("POLL_INTERVAL_S", "60"))
TRIP_UPDATES_POLL_INTERVAL_S = int(
    os.getenv("TRIP_UPDATES_POLL_INTERVAL_S", str(POLL_INTERVAL_S))
)
VEHICLE_POSITIONS_POLL_INTERVAL_S = int(
    os.getenv("VEHICLE_POSITIONS_POLL_INTERVAL_S", str(POLL_INTERVAL_S))
)
# TripUpdates ignores VehiclePositions trip assignments older than this
# (e.g. while that feed is failing) and picks trips from its own data.
VEHICLE_TRIPS_MAX_AGE_S = max(120, 2 * VEHICLE_POSITIONS_POLL_INTERVAL_S)

HTTP_TIMEOUT_S = 20
# Backoff after consecutive failed LTC requests (rate limiting, outages): the
# feed's poll interval, then doubling up to this cap. A Retry-After header is
# honoured up to MAX_RETRY_AFTER_S.
MAX_BACKOFF_S = 600
MAX_RETRY_AFTER_S = 3600
STATS_INTERVAL_S = 3600
STATIC_GTFS_CHECK_INTERVAL_S = 86400
STATIC_GTFS_RETRY_S = 3600  # after a failed check

log = logging.getLogger("ltc-poller")


# --------------------------------------------------------------------------- #
# Realtime feeds
# --------------------------------------------------------------------------- #

class LatestVehicleTrips:
    """The newest VehiclePositions trip assignments, shared with TripUpdates."""

    def __init__(self):
        self._lock = threading.Lock()
        self._vehicle_trips: feeds.VehicleTrips | None = None
        self._confirmed_at = 0.0

    def set(self, vehicle_trips: feeds.VehicleTrips) -> None:
        with self._lock:
            self._vehicle_trips = vehicle_trips
            self._confirmed_at = time.monotonic()

    def confirm(self) -> None:
        """The feed hasn't changed, so the current assignments are still fresh."""
        with self._lock:
            self._confirmed_at = time.monotonic()

    def get(self) -> feeds.VehicleTrips | None:
        """The assignments, or None if there are none from the last VEHICLE_TRIPS_MAX_AGE_S."""
        with self._lock:
            if time.monotonic() - self._confirmed_at > VEHICLE_TRIPS_MAX_AGE_S:
                return None
            return self._vehicle_trips


class FeedPoller:
    """Fetches one realtime feed and stores each new version of it once.

    Unchanged feeds are skipped by ETag (304) and by feed timestamp.
    """

    name: str
    url: str
    field: str  # document field the feed is stored under

    def __init__(self, collection: Collection, latest_vehicle_trips: LatestVehicleTrips):
        self.collection = collection
        self.latest_vehicle_trips = latest_vehicle_trips
        self.session = requests.Session()
        self.etag: str | None = None
        self.last_stored_time: datetime | None = db.latest_ts(collection)

    def _get(self) -> requests.Response:
        headers = {"If-None-Match": self.etag} if self.etag else {}
        response = self.session.get(self.url, headers=headers, timeout=HTTP_TIMEOUT_S)
        if response.status_code != 304:
            response.raise_for_status()
        return response

    def _trim(self, raw: bytes) -> tuple[gtfs.FeedMessage, int]:
        """Return (trimmed feed, vehicle count)."""
        raise NotImplementedError

    def _on_unchanged(self) -> None:
        pass

    def _is_stored(self, feed_time: datetime) -> bool:
        return self.last_stored_time is not None and feed_time <= self.last_stored_time

    def poll_once(self) -> None:
        response = self._get()
        if response.status_code == 304:
            self._on_unchanged()
            log.info("%s unchanged since last poll; skipping", self.name)
            return
        # Only remember the ETag once the snapshot is safely stored, so a
        # failed insert (e.g. Mongo restarting) is retried next poll, not 304'd.
        etag = response.headers.get("ETag")

        feed, vehicle_count = self._trim(response.content)
        feed_time = datetime.fromtimestamp(feed.header.timestamp, tz=timezone.utc)
        if self._is_stored(feed_time):
            self.etag = etag
            log.info(
                "%s timestamp %s already stored; skipping", self.name, feed_time.isoformat()
            )
            return

        snapshot = db.make_snapshot(feed_time, vehicle_count, self.field, feed)
        if not db.insert_snapshot(self.collection, snapshot):
            log.info("%s snapshot %s already stored", self.name, feed_time.isoformat())
        self.etag = etag
        self.last_stored_time = feed_time
        log.info(
            "stored %s %s: %d vehicles, %d B (raw %d B)",
            self.name, feed_time.isoformat(), vehicle_count,
            len(snapshot[self.field]), len(response.content),
        )


class TripUpdatesPoller(FeedPoller):
    name = "TripUpdates"
    url = TRIP_UPDATES_URL
    field = "tu"

    def _trim(self, raw: bytes) -> tuple[gtfs.FeedMessage, int]:
        return feeds.trim_trip_updates(raw, self.latest_vehicle_trips.get())


class VehiclePositionsPoller(FeedPoller):
    name = "VehiclePositions"
    url = VEHICLE_POSITIONS_URL
    field = "vp"

    def _trim(self, raw: bytes) -> tuple[gtfs.FeedMessage, int]:
        vehicle_positions = feeds.trim_vehicle_positions(raw)
        # Shared before storing, so TripUpdates gets it even if Mongo is down.
        self.latest_vehicle_trips.set(feeds.vehicle_trips(vehicle_positions))
        return vehicle_positions, len(vehicle_positions.entity)

    def _on_unchanged(self) -> None:
        self.latest_vehicle_trips.confirm()


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


def backoff_s(
    poll_interval_s: int, consecutive_failures: int, error: requests.RequestException
) -> float:
    delay_s = min(poll_interval_s * 2 ** (consecutive_failures - 1), MAX_BACKOFF_S)
    requested_delay_s = retry_after_s(error)
    if requested_delay_s is not None:
        delay_s = max(delay_s, min(requested_delay_s, MAX_RETRY_AFTER_S))
    return delay_s


def realtime_retry_delay(poll_interval_s: int) -> Callable[[int, Exception], float | None]:
    """Back off on failed LTC requests; retry anything else on schedule."""
    def retry_delay_s(consecutive_failures: int, error: Exception) -> float | None:
        if isinstance(error, requests.RequestException):  # rate limited, LTC down, network
            return backoff_s(poll_interval_s, consecutive_failures, error)
        return None  # Mongo restarting (pymongo reconnects) or a bad feed
    return retry_delay_s


def update_static_gtfs(database: Database, session: requests.Session) -> None:
    """Import a new static GTFS version if there is one and expire old ones."""
    static_gtfs.check_for_update(database, session)
    static_gtfs.delete_expired_versions(database)


# --------------------------------------------------------------------------- #
# Jobs
# --------------------------------------------------------------------------- #

class Shutdown:
    """Set on SIGINT/SIGTERM; offers a sleep that wakes up on it."""

    def __init__(self):
        self._event = threading.Event()
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    @property
    def stopping(self) -> bool:
        return self._event.is_set()

    def _stop(self, *_) -> None:
        if not self.stopping:
            log.info("Shutting down after current polls")
        self._event.set()

    def sleep(self, seconds: float) -> None:
        self._event.wait(max(0.0, seconds))


class Job(threading.Thread):
    """Runs `run_once` every `interval_s` in its own thread until shutdown.

    After a failure, `retry_delay_s(consecutive_failures, error)` returns how
    long to wait before trying again, or None to keep the normal schedule.
    """

    def __init__(
        self,
        name: str,
        interval_s: float,
        run_once: Callable[[], None],
        retry_delay_s: Callable[[int, Exception], float | None],
        shutdown: Shutdown,
    ):
        super().__init__(name=name)
        self.interval_s = interval_s
        self.run_once = run_once
        self.retry_delay_s = retry_delay_s
        self.shutdown = shutdown

    def _log_failure(
        self, error: Exception, consecutive_failures: int, delay_s: float | None
    ) -> None:
        retry = f"retrying in {delay_s:.0f}s" if delay_s is not None else "retrying on schedule"
        if isinstance(error, requests.RequestException):
            log.warning(
                "%s request failed (%d in a row): %s; %s",
                self.name, consecutive_failures, error, retry,
            )
        elif isinstance(error, PyMongoError):
            log.warning("%s failed, MongoDB unavailable: %s; %s", self.name, error, retry)
        else:
            log.exception("%s failed; %s", self.name, retry)

    def run(self) -> None:
        next_run_at = time.monotonic()
        consecutive_failures = 0
        while not self.shutdown.stopping:
            try:
                self.run_once()
            except Exception as error:
                consecutive_failures += 1
                delay_s = self.retry_delay_s(consecutive_failures, error)
                self._log_failure(error, consecutive_failures, delay_s)
                if delay_s is not None:
                    self.shutdown.sleep(delay_s)
                    next_run_at = time.monotonic()  # resume the normal schedule from here
                    continue
            else:
                if consecutive_failures:
                    log.info(
                        "%s recovered after %d failed attempts", self.name, consecutive_failures
                    )
                    consecutive_failures = 0

            next_run_at += self.interval_s
            self.shutdown.sleep(next_run_at - time.monotonic())
            if time.monotonic() - next_run_at > self.interval_s:
                next_run_at = time.monotonic()  # fell far behind; don't burst-run


def connect_with_retry(
    client, shutdown: Shutdown, latest_vehicle_trips: LatestVehicleTrips
) -> tuple[TripUpdatesPoller, VehiclePositionsPoller] | None:
    """Set up the collections and pollers, retrying with backoff.

    Returns None if shut down before MongoDB became reachable.

    pymongo reconnects on its own once connected; this only covers startup,
    e.g. Mongo still coming up or temporarily down.
    """
    retry_delay_s = 1
    while True:
        try:
            trip_updates_collection, vehicle_positions_collection = db.open_collections(client)
            return (
                TripUpdatesPoller(trip_updates_collection, latest_vehicle_trips),
                VehiclePositionsPoller(vehicle_positions_collection, latest_vehicle_trips),
            )
        except PyMongoError as error:
            log.warning("MongoDB unavailable (%s); retrying in %ds", error, retry_delay_s)
            shutdown.sleep(retry_delay_s)
            if shutdown.stopping:
                return None
            retry_delay_s = min(retry_delay_s * 2, 60)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
    )
    shutdown = Shutdown()
    client = db.connect()
    try:
        pollers = connect_with_retry(client, shutdown, LatestVehicleTrips())
        if pollers is None:
            return
        trip_updates_poller, vehicle_positions_poller = pollers
        database = trip_updates_poller.collection.database
        log.info("Connected to MongoDB")

        jobs = []
        storage = []
        if STORE_VEHICLE_POSITIONS:
            # Started first so TripUpdates usually has trip assignments to use.
            jobs.append(Job(
                "vehicle-positions",
                VEHICLE_POSITIONS_POLL_INTERVAL_S,
                vehicle_positions_poller.poll_once,
                realtime_retry_delay(VEHICLE_POSITIONS_POLL_INTERVAL_S),
                shutdown,
            ))
            storage.append((vehicle_positions_poller.collection, VEHICLE_POSITIONS_POLL_INTERVAL_S))
        jobs.append(Job(
            "trip-updates",
            TRIP_UPDATES_POLL_INTERVAL_S,
            trip_updates_poller.poll_once,
            realtime_retry_delay(TRIP_UPDATES_POLL_INTERVAL_S),
            shutdown,
        ))
        storage.append((trip_updates_poller.collection, TRIP_UPDATES_POLL_INTERVAL_S))
        static_gtfs_session = requests.Session()
        jobs.append(Job(
            "static-gtfs",
            STATIC_GTFS_CHECK_INTERVAL_S,
            lambda: update_static_gtfs(database, static_gtfs_session),
            lambda *_: STATIC_GTFS_RETRY_S,
            shutdown,
        ))
        jobs.append(Job(
            "storage-stats",
            STATS_INTERVAL_S,
            lambda: db.log_storage(storage),
            lambda *_: None,
            shutdown,
        ))

        log.info(
            "Polling TripUpdates every %ds, VehiclePositions %s, static GTFS daily "
            "into %s (retention %d days)",
            TRIP_UPDATES_POLL_INTERVAL_S,
            f"every {VEHICLE_POSITIONS_POLL_INTERVAL_S}s" if STORE_VEHICLE_POSITIONS else "off",
            db.MONGO_DB, db.RETENTION_DAYS,
        )
        for job in jobs:
            job.start()
        # Join with a timeout so the main thread keeps handling signals.
        while any(job.is_alive() for job in jobs):
            for job in jobs:
                job.join(timeout=1)
    finally:
        client.close()


if __name__ == "__main__":
    main()

"""MongoDB storage for trimmed feed snapshots.

Storage model: one document per new feed version, one collection per feed,
since each feed is polled on its own.

    trip updates collection ("snapshots"):
    {
      ts:  Date     # feed header timestamp (indexed, TTL)
      n:   int      # vehicles assigned to a trip
      tu:  Binary   # trimmed TripUpdates FeedMessage (protobuf)
    }

    vehicle positions collection ("vehicle_positions"):
    {
      ts:  Date     # feed header timestamp (indexed, TTL)
      n:   int      # vehicles with a position
      vp:  Binary   # trimmed VehiclePositions FeedMessage (protobuf)
    }

Snapshots stored before the feeds were split also have `vp` in the trip
updates collection. Only `ts` needs to be queried/indexed, so everything else
stays as compact protobuf bytes; decode it with `decode_snapshot()` when
building dashboard metrics.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from bson import Binary
from google.transit import gtfs_realtime_pb2 as gtfs
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError, OperationFailure

from feeds import parse_feed

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "ltc")
# MONGO_COLLECTION is the older name for TRIP_UPDATES_COLLECTION.
TRIP_UPDATES_COLLECTION = os.getenv(
    "TRIP_UPDATES_COLLECTION", os.getenv("MONGO_COLLECTION", "snapshots")
)
VEHICLE_POSITIONS_COLLECTION = os.getenv(
    "VEHICLE_POSITIONS_COLLECTION", "vehicle_positions"
)

# TODO: Tune once real usage is known. The poller logs measured storage and a
# projection for this retention every hour; raise this if the projection is
# comfortably under the 10-15 GB budget. Changing it and restarting the poller
# updates the existing TTL index in place (collMod).
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "35"))
STORAGE_BUDGET_GB = float(os.getenv("STORAGE_BUDGET_GB", "12"))

TTL_INDEX_NAME = "ts_ttl"
INDEX_CONFLICT_CODES = (85, 86)  # IndexOptionsConflict / IndexKeySpecsConflict

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Connection / setup
# --------------------------------------------------------------------------- #

def connect(uri: str = MONGO_URI) -> MongoClient:
    return MongoClient(uri, serverSelectionTimeoutMS=10_000)


def ensure_collection(database: Database, name: str) -> Collection:
    """Return a snapshots collection, creating or updating its TTL index."""
    collection = database[name]
    ttl_s = RETENTION_DAYS * 86400
    try:
        collection.create_index(
            [("ts", ASCENDING)],
            unique=True,
            expireAfterSeconds=ttl_s,
            name=TTL_INDEX_NAME,
        )
    except OperationFailure as error:
        # Index already exists with a different TTL: update it in place.
        if error.code not in INDEX_CONFLICT_CODES:
            raise
        database.command(
            "collMod",
            name,
            index={"name": TTL_INDEX_NAME, "expireAfterSeconds": ttl_s},
        )
        log.info("Updated %s TTL index to %d days", name, RETENTION_DAYS)
    return collection


def open_collections(client: MongoClient) -> tuple[Collection, Collection]:
    """Return the (trip updates, vehicle positions) collections."""
    database = client[MONGO_DB]
    return (
        ensure_collection(database, TRIP_UPDATES_COLLECTION),
        ensure_collection(database, VEHICLE_POSITIONS_COLLECTION),
    )


# --------------------------------------------------------------------------- #
# Snapshot documents
# --------------------------------------------------------------------------- #

def make_snapshot(
    feed_time: datetime, vehicle_count: int, field: str, feed: gtfs.FeedMessage
) -> dict:
    """A document storing `feed` under `field` ("tu" or "vp")."""
    return {"ts": feed_time, "n": vehicle_count, field: Binary(feed.SerializeToString())}


def decode_snapshot(
    snapshot: dict,
) -> tuple[gtfs.FeedMessage | None, gtfs.FeedMessage | None]:
    """Turn a stored document back into (trip_updates, vehicle_positions).

    Each is None if the document doesn't hold that feed.
    """
    trip_updates = parse_feed(snapshot["tu"]) if snapshot.get("tu") is not None else None
    vehicle_positions = (
        parse_feed(snapshot["vp"]) if snapshot.get("vp") is not None else None
    )
    return trip_updates, vehicle_positions


def latest_ts(collection: Collection) -> datetime | None:
    """Timestamp of the newest stored snapshot, as an aware UTC datetime."""
    newest = collection.find_one(sort=[("ts", -1)], projection={"ts": 1})
    if newest is None:
        return None
    # pymongo returns naive datetimes (in UTC) by default.
    return newest["ts"].replace(tzinfo=timezone.utc)


def insert_snapshot(collection: Collection, snapshot: dict) -> bool:
    """Store a snapshot. Returns False if one with the same `ts` exists."""
    try:
        collection.insert_one(snapshot)
    except DuplicateKeyError:
        return False
    return True


# --------------------------------------------------------------------------- #
# Storage stats
# --------------------------------------------------------------------------- #

def storage_stats(collection: Collection) -> tuple[int, int]:
    """Return (document count, bytes on disk including indexes)."""
    stats = next(
        collection.aggregate([{"$collStats": {"storageStats": {}}}])
    )["storageStats"]
    doc_count = stats.get("count", 0)
    bytes_on_disk = stats.get("storageSize", 0) + stats.get("totalIndexSize", 0)
    return doc_count, bytes_on_disk


def log_storage(collections: list[tuple[Collection, int]]) -> None:
    """Log storage per (collection, poll interval) and the combined projection.

    Assumes one document per poll, so feeds that often don't change between
    polls come out pessimistic.
    """
    bytes_per_day = 0.0
    for collection, poll_interval_s in collections:
        doc_count, bytes_on_disk = storage_stats(collection)
        if not doc_count:
            continue
        bytes_per_doc = bytes_on_disk / doc_count
        collection_bytes_per_day = bytes_per_doc * 86400 / poll_interval_s
        bytes_per_day += collection_bytes_per_day
        log.info(
            "storage %s: %d docs, %.1f MB on disk (%.1f KB/doc) -> ~%.2f GB "
            "for %d-day retention",
            collection.name, doc_count, bytes_on_disk / 1e6, bytes_per_doc / 1e3,
            collection_bytes_per_day * RETENTION_DAYS / 1e9, RETENTION_DAYS,
        )
    if not bytes_per_day:
        return
    log.info(
        "storage total: %d-day retention -> ~%.2f GB; %.0f GB budget fits ~%.0f days",
        RETENTION_DAYS, bytes_per_day * RETENTION_DAYS / 1e9,
        STORAGE_BUDGET_GB, STORAGE_BUDGET_GB * 1e9 / bytes_per_day,
    )
    # Small collections are dominated by fixed overhead, so treat early
    # projections as pessimistic.

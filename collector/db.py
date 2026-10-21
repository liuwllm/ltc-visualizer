"""MongoDB storage for trimmed feed snapshots.

Storage model: one document per poll.

    {
      ts:  Date     # feed header timestamp of TripUpdates (indexed, TTL)
      n:   int      # number of active vehicles in this snapshot
      tu:  Binary   # trimmed TripUpdates FeedMessage (protobuf)
      vp:  Binary   # trimmed VehiclePositions FeedMessage (protobuf), optional
    }

Only `ts` needs to be queried/indexed, so everything else stays as compact
protobuf bytes; decode it with `decode_snapshot()` when building dashboard
metrics.
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
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "snapshots")

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


def ensure_collection(database: Database) -> Collection:
    """Return the snapshots collection, creating or updating its TTL index."""
    collection = database[MONGO_COLLECTION]
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
            MONGO_COLLECTION,
            index={"name": TTL_INDEX_NAME, "expireAfterSeconds": ttl_s},
        )
        log.info("Updated TTL index to %d days", RETENTION_DAYS)
    return collection


def open_collection(client: MongoClient) -> Collection:
    return ensure_collection(client[MONGO_DB])


# --------------------------------------------------------------------------- #
# Snapshot documents
# --------------------------------------------------------------------------- #

def make_snapshot(
    feed_time: datetime,
    active_vehicle_count: int,
    trip_updates: gtfs.FeedMessage,
    vehicle_positions: gtfs.FeedMessage | None = None,
) -> dict:
    snapshot = {
        "ts": feed_time,
        "n": active_vehicle_count,
        "tu": Binary(trip_updates.SerializeToString()),
    }
    if vehicle_positions is not None:
        snapshot["vp"] = Binary(vehicle_positions.SerializeToString())
    return snapshot


def decode_snapshot(snapshot: dict) -> tuple[gtfs.FeedMessage, gtfs.FeedMessage | None]:
    """Turn a stored document back into (trip_updates, vehicle_positions)."""
    trip_updates = parse_feed(snapshot["tu"])
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


def log_storage(collection: Collection, poll_interval_s: int) -> None:
    doc_count, bytes_on_disk = storage_stats(collection)
    if not doc_count:
        return
    bytes_per_doc = bytes_on_disk / doc_count
    polls_per_day = 86400 / poll_interval_s
    projected_gb = bytes_per_doc * polls_per_day * RETENTION_DAYS / 1e9
    days_within_budget = STORAGE_BUDGET_GB * 1e9 / (bytes_per_doc * polls_per_day)
    log.info(
        "storage: %d docs, %.1f MB on disk (%.1f KB/doc). %d-day retention "
        "-> ~%.2f GB; %.0f GB budget fits ~%.0f days",
        doc_count, bytes_on_disk / 1e6, bytes_per_doc / 1e3, RETENTION_DAYS,
        projected_gb, STORAGE_BUDGET_GB, days_within_budget,
    )
    # Small collections are dominated by fixed overhead, so treat early
    # projections as pessimistic.

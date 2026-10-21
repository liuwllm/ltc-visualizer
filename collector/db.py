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


def ensure_collection(db: Database) -> Collection:
    """Return the snapshots collection, creating or updating its TTL index."""
    coll = db[MONGO_COLLECTION]
    ttl = RETENTION_DAYS * 86400
    try:
        coll.create_index(
            [("ts", ASCENDING)],
            unique=True,
            expireAfterSeconds=ttl,
            name=TTL_INDEX_NAME,
        )
    except OperationFailure as e:
        # Index already exists with a different TTL: update it in place.
        if e.code not in INDEX_CONFLICT_CODES:
            raise
        db.command(
            "collMod",
            MONGO_COLLECTION,
            index={"name": TTL_INDEX_NAME, "expireAfterSeconds": ttl},
        )
        log.info("Updated TTL index to %d days", RETENTION_DAYS)
    return coll


def open_collection(client: MongoClient) -> Collection:
    return ensure_collection(client[MONGO_DB])


# --------------------------------------------------------------------------- #
# Snapshot documents
# --------------------------------------------------------------------------- #

def make_snapshot(
    ts: datetime,
    n_active: int,
    tu: gtfs.FeedMessage,
    vp: gtfs.FeedMessage | None = None,
) -> dict:
    doc = {"ts": ts, "n": n_active, "tu": Binary(tu.SerializeToString())}
    if vp is not None:
        doc["vp"] = Binary(vp.SerializeToString())
    return doc


def decode_snapshot(doc: dict) -> tuple[gtfs.FeedMessage, gtfs.FeedMessage | None]:
    """Turn a stored document back into (trip_updates, vehicle_positions)."""
    tu = parse_feed(doc["tu"])
    vp = parse_feed(doc["vp"]) if doc.get("vp") is not None else None
    return tu, vp


def latest_ts(coll: Collection) -> datetime | None:
    """Timestamp of the newest stored snapshot, as an aware UTC datetime."""
    last = coll.find_one(sort=[("ts", -1)], projection={"ts": 1})
    if last is None:
        return None
    # pymongo returns naive datetimes (in UTC) by default.
    return last["ts"].replace(tzinfo=timezone.utc)


def insert_snapshot(coll: Collection, doc: dict) -> bool:
    """Store a snapshot. Returns False if one with the same `ts` exists."""
    try:
        coll.insert_one(doc)
    except DuplicateKeyError:
        return False
    return True


# --------------------------------------------------------------------------- #
# Storage stats
# --------------------------------------------------------------------------- #

def storage_stats(coll: Collection) -> tuple[int, int]:
    """Return (document count, bytes on disk including indexes)."""
    stats = next(coll.aggregate([{"$collStats": {"storageStats": {}}}]))["storageStats"]
    count = stats.get("count", 0)
    on_disk = stats.get("storageSize", 0) + stats.get("totalIndexSize", 0)
    return count, on_disk


def log_storage(coll: Collection, poll_interval_s: int) -> None:
    count, on_disk = storage_stats(coll)
    if not count:
        return
    per_doc = on_disk / count
    polls_per_day = 86400 / poll_interval_s
    projected_gb = per_doc * polls_per_day * RETENTION_DAYS / 1e9
    max_days = STORAGE_BUDGET_GB * 1e9 / (per_doc * polls_per_day)
    log.info(
        "storage: %d docs, %.1f MB on disk (%.1f KB/doc). %d-day retention "
        "-> ~%.2f GB; %.0f GB budget fits ~%.0f days",
        count, on_disk / 1e6, per_doc / 1e3, RETENTION_DAYS,
        projected_gb, STORAGE_BUDGET_GB, max_days,
    )
    # Small collections are dominated by fixed overhead, so treat early
    # projections as pessimistic.

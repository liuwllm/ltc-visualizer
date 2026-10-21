"""Archive LTC's static GTFS in MongoDB and look things up in it.

Realtime snapshots only carry ids (trip_id, stop_id, route_id). Direction,
shape and each trip's first stop -- needed for bunching -- come from the static
GTFS, and those ids only mean something within the version that was in effect,
so every version is kept until no stored snapshot can refer to it.

* A version is identified by a hash of the *contents* of every file in the
  zip, not by feed_version or the HTTP ETag: any changed file is a new version,
  and a re-uploaded but identical zip is not.
* The zip is checked daily with a conditional GET (If-None-Match /
  If-Modified-Since), so an unchanged file isn't downloaded.
* Each version keeps the original zip (GridFS, so any file can be read with
  read_table) plus lookup collections for trips, shapes, stops and routes.
* A version is deleted once its end date is more than RETENTION_DAYS ago, i.e.
  along with the last snapshots that could use it. The version LTC currently
  serves is always kept.

Collections, all keyed by `version` (the content hash). Dates are GTFS
"YYYYMMDD" strings, which compare correctly as strings.

    gtfs_versions  {_id: version, feed_version, start_date, end_date,
                    first_seen_at, last_seen_at, etag, last_modified,
                    size_bytes, files: {name: sha256}}
    gtfs_trips     {version, trip_id, route_id, service_id, direction_id,
                    shape_id, block_id, headsign,
                    first_stop_sequence, last_stop_sequence}
    gtfs_shapes    {version, shape_id, points: [[lon, lat], ...],
                    dist_m: [...]}  # cumulative distance along the shape
    gtfs_stops     {version, stop_id, code, name, lat, lon}
    gtfs_routes    {version, route_id, short_name, long_name, color}
    gtfs_files     GridFS bucket holding each version's zip (_id = version)
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import math
import os
import zipfile
from collections.abc import Iterable, Iterator
from datetime import datetime, timedelta, timezone

import gridfs
import requests
from pymongo import ASCENDING, DESCENDING
from pymongo.database import Database

import db

STATIC_GTFS_URL = os.getenv(
    "STATIC_GTFS_URL", "https://www.londontransit.ca/gtfsfeed/google_transit.zip"
)
HTTP_TIMEOUT_S = 60

VERSIONS = "gtfs_versions"
TRIPS = "gtfs_trips"
SHAPES = "gtfs_shapes"
STOPS = "gtfs_stops"
ROUTES = "gtfs_routes"
FILES_BUCKET = "gtfs_files"
# lookup collection -> the id field it is keyed by within a version
TABLE_KEYS = {TRIPS: "trip_id", SHAPES: "shape_id", STOPS: "stop_id", ROUTES: "route_id"}

EARTH_RADIUS_M = 6_371_000

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #

def _read_zip(content: bytes) -> dict[str, bytes]:
    """Map file name -> contents for every file in the zip."""
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        return {
            info.filename: archive.read(info)
            for info in archive.infolist()
            if not info.is_dir()
        }


def content_hash(files: dict[str, bytes]) -> tuple[str, dict[str, str]]:
    """Return (version id, {file name: sha256}) for the zip's contents.

    Hashes what the files contain, not the zip itself, so re-zipping identical
    files (new timestamps, compression) doesn't count as a new version.
    """
    file_hashes = {
        name: hashlib.sha256(data).hexdigest() for name, data in sorted(files.items())
    }
    combined = hashlib.sha256()
    for name, digest in file_hashes.items():
        combined.update(f"{name}\0{digest}\n".encode())
    return combined.hexdigest(), file_hashes


def _rows(files: dict[str, bytes], name: str) -> Iterator[dict[str, str]]:
    """Rows of one GTFS file, with whitespace stripped (LTC pads some times)."""
    if name not in files:
        return
    reader = csv.DictReader(io.StringIO(files[name].decode("utf-8-sig")))
    for row in reader:
        yield {
            key.strip(): (value or "").strip()
            for key, value in row.items()
            if key is not None
        }


def _int_or_none(value: str | None) -> int | None:
    return int(value) if value else None


def _float_or_none(value: str | None) -> float | None:
    return float(value) if value else None


def _service_date_range(files: dict[str, bytes]) -> tuple[str | None, str | None]:
    """(start, end) dates the version covers: feed_info, else the calendars."""
    feed_info = next(_rows(files, "feed_info.txt"), {})
    if feed_info.get("feed_start_date") and feed_info.get("feed_end_date"):
        return feed_info["feed_start_date"], feed_info["feed_end_date"]
    dates = [
        date
        for row in _rows(files, "calendar.txt")
        for date in (row.get("start_date"), row.get("end_date"))
        if date
    ]
    dates += [row["date"] for row in _rows(files, "calendar_dates.txt") if row.get("date")]
    return (min(dates), max(dates)) if dates else (None, None)


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = phi2 - phi1
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def _trip_docs(files: dict[str, bytes], version: str) -> Iterator[dict]:
    first_stop_sequence: dict[str, int] = {}
    last_stop_sequence: dict[str, int] = {}
    for row in _rows(files, "stop_times.txt"):
        trip_id = row["trip_id"]
        stop_sequence = int(row["stop_sequence"])
        if stop_sequence < first_stop_sequence.get(trip_id, stop_sequence + 1):
            first_stop_sequence[trip_id] = stop_sequence
        if stop_sequence > last_stop_sequence.get(trip_id, stop_sequence - 1):
            last_stop_sequence[trip_id] = stop_sequence

    for row in _rows(files, "trips.txt"):
        trip_id = row["trip_id"]
        yield {
            "version": version,
            "trip_id": trip_id,
            "route_id": row.get("route_id") or None,
            "service_id": row.get("service_id") or None,
            "direction_id": _int_or_none(row.get("direction_id")),
            "shape_id": row.get("shape_id") or None,
            "block_id": row.get("block_id") or None,
            "headsign": row.get("trip_headsign") or None,
            "first_stop_sequence": first_stop_sequence.get(trip_id),
            "last_stop_sequence": last_stop_sequence.get(trip_id),
        }


def _shape_docs(files: dict[str, bytes], version: str) -> Iterator[dict]:
    # shape id -> [(sequence, lon, lat)]
    points_by_shape: dict[str, list[tuple[int, float, float]]] = {}
    for row in _rows(files, "shapes.txt"):
        points_by_shape.setdefault(row["shape_id"], []).append(
            (
                int(row["shape_pt_sequence"]),
                float(row["shape_pt_lon"]),
                float(row["shape_pt_lat"]),
            )
        )

    for shape_id, points in points_by_shape.items():
        points.sort()
        coordinates = [[lon, lat] for _, lon, lat in points]
        # LTC's shapes.txt has no shape_dist_traveled, so compute it.
        dist_m = [0.0]
        for (lon1, lat1), (lon2, lat2) in zip(coordinates, coordinates[1:]):
            dist_m.append(dist_m[-1] + haversine_m(lat1, lon1, lat2, lon2))
        yield {
            "version": version,
            "shape_id": shape_id,
            "points": coordinates,
            "dist_m": [round(distance, 1) for distance in dist_m],
        }


def _stop_docs(files: dict[str, bytes], version: str) -> Iterator[dict]:
    for row in _rows(files, "stops.txt"):
        yield {
            "version": version,
            "stop_id": row["stop_id"],
            "code": row.get("stop_code") or None,
            "name": row.get("stop_name") or None,
            "lat": _float_or_none(row.get("stop_lat")),
            "lon": _float_or_none(row.get("stop_lon")),
        }


def _route_docs(files: dict[str, bytes], version: str) -> Iterator[dict]:
    for row in _rows(files, "routes.txt"):
        yield {
            "version": version,
            "route_id": row["route_id"],
            "short_name": row.get("route_short_name") or None,
            "long_name": row.get("route_long_name") or None,
            "color": row.get("route_color") or None,
        }


# --------------------------------------------------------------------------- #
# Updating
# --------------------------------------------------------------------------- #

def ensure_indexes(database: Database) -> None:
    database[VERSIONS].create_index([("start_date", ASCENDING), ("end_date", ASCENDING)])
    database[VERSIONS].create_index([("last_seen_at", DESCENDING)])
    for table, key in TABLE_KEYS.items():
        database[table].create_index([("version", ASCENDING), (key, ASCENDING)], unique=True)


def _files_bucket(database: Database) -> gridfs.GridFS:
    return gridfs.GridFS(database, collection=FILES_BUCKET)


def _delete_version_data(database: Database, version: str) -> None:
    for table in TABLE_KEYS:
        database[table].delete_many({"version": version})
    bucket = _files_bucket(database)
    if bucket.exists(version):
        bucket.delete(version)


def _import_version(
    database: Database,
    version: str,
    content: bytes,
    files: dict[str, bytes],
    file_hashes: dict[str, str],
    validators: dict,
) -> None:
    # Clear anything left by an interrupted import of the same version.
    _delete_version_data(database, version)
    _files_bucket(database).put(
        content, _id=version, filename=os.path.basename(STATIC_GTFS_URL)
    )
    counts = {}
    for table, docs in (
        (TRIPS, _trip_docs(files, version)),
        (SHAPES, _shape_docs(files, version)),
        (STOPS, _stop_docs(files, version)),
        (ROUTES, _route_docs(files, version)),
    ):
        docs = list(docs)
        if docs:
            database[table].insert_many(docs, ordered=False)
        counts[table] = len(docs)

    start_date, end_date = _service_date_range(files)
    feed_info = next(_rows(files, "feed_info.txt"), {})
    # Written last: a version only becomes visible once all its data is in.
    database[VERSIONS].insert_one({
        "_id": version,
        "feed_version": feed_info.get("feed_version") or None,
        "start_date": start_date,
        "end_date": end_date,
        "first_seen_at": validators["last_seen_at"],
        "size_bytes": len(content),
        "files": file_hashes,
        **validators,
    })
    log.info(
        "Imported static GTFS %s (%s, %s-%s): %d trips, %d shapes, %d stops, %d routes",
        version[:12], feed_info.get("feed_version"), start_date, end_date,
        counts[TRIPS], counts[SHAPES], counts[STOPS], counts[ROUTES],
    )


def check_for_update(database: Database, session: requests.Session) -> str | None:
    """Download the static GTFS if it changed and import it if its contents did.

    Returns the new version id, or None if nothing changed.
    """
    ensure_indexes(database)
    versions = database[VERSIONS]
    current = latest_version(database)
    headers = {}
    if current is not None:
        if current.get("etag"):
            headers["If-None-Match"] = current["etag"]
        if current.get("last_modified"):
            headers["If-Modified-Since"] = current["last_modified"]

    response = session.get(STATIC_GTFS_URL, headers=headers, timeout=HTTP_TIMEOUT_S)
    now = datetime.now(timezone.utc)
    if response.status_code == 304:
        versions.update_one({"_id": current["_id"]}, {"$set": {"last_seen_at": now}})
        log.info("Static GTFS unchanged (%s)", current.get("feed_version"))
        return None
    response.raise_for_status()

    files = _read_zip(response.content)
    version, file_hashes = content_hash(files)
    validators = {
        "etag": response.headers.get("ETag"),
        "last_modified": response.headers.get("Last-Modified"),
        "last_seen_at": now,
    }
    if versions.find_one({"_id": version}, projection={"_id": 1}) is not None:
        # Same contents (re-uploaded, or LTC went back to an older version).
        versions.update_one({"_id": version}, {"$set": validators})
        log.info("Static GTFS downloaded but contents unchanged (%s)", version[:12])
        return None

    _import_version(database, version, response.content, files, file_hashes, validators)
    return version


def delete_expired_versions(database: Database) -> list[str]:
    """Delete versions no retained snapshot can use; returns their ids.

    A version expires once its end date (or, without one, the last time LTC
    served it) is more than RETENTION_DAYS ago. The current one is kept.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=db.RETENTION_DAYS)).strftime("%Y%m%d")
    current = latest_version(database)
    expired = []
    for version in database[VERSIONS].find({}, projection={"end_date": 1, "last_seen_at": 1}):
        if current is not None and version["_id"] == current["_id"]:
            continue
        end_date = version.get("end_date") or version["last_seen_at"].strftime("%Y%m%d")
        if end_date < cutoff:
            expired.append(version["_id"])

    for version in expired:
        # Version record first, so a half-deleted version is never looked up.
        database[VERSIONS].delete_one({"_id": version})
        _delete_version_data(database, version)
        log.info("Deleted expired static GTFS version %s", version[:12])
    return expired


# --------------------------------------------------------------------------- #
# Lookups
# --------------------------------------------------------------------------- #

def latest_version(database: Database) -> dict | None:
    """The version LTC served most recently."""
    return database[VERSIONS].find_one(sort=[("last_seen_at", DESCENDING)])


def versions_for_date(database: Database, service_date: str) -> list[dict]:
    """Versions covering service_date ("YYYYMMDD"), most recently published first."""
    return list(
        database[VERSIONS]
        .find({"start_date": {"$lte": service_date}, "end_date": {"$gte": service_date}})
        .sort("first_seen_at", DESCENDING)
    )


def version_for_date(database: Database, service_date: str) -> str | None:
    """Id of the most recently published version covering service_date."""
    versions = versions_for_date(database, service_date)
    return versions[0]["_id"] if versions else None


def resolve_trips(
    database: Database, service_date: str, trip_ids: Iterable[str]
) -> dict[str, dict]:
    """Map realtime trip ids to their static trip info, including `version`.

    Pass the trip's start_date from TripUpdates as service_date. When several
    versions cover the date (a mid-period revision), the most recently
    published one containing the trip wins. Trips not in any covering version
    are left out rather than matched against an unrelated version.
    """
    remaining = set(trip_ids)
    found: dict[str, dict] = {}
    for version in versions_for_date(database, service_date):
        if not remaining:
            break
        for trip in database[TRIPS].find(
            {"version": version["_id"], "trip_id": {"$in": list(remaining)}},
            projection={"_id": 0},
        ):
            found[trip["trip_id"]] = trip
            remaining.discard(trip["trip_id"])
    return found


def is_before_first_stop(trip: dict, stop_sequence: int) -> bool:
    """True if a bus whose next stop is stop_sequence hasn't started the trip,
    e.g. it is waiting at the terminal. Such buses aren't bunched."""
    first = trip.get("first_stop_sequence")
    return first is not None and stop_sequence <= first


def _find(
    database: Database, table: str, version: str, ids: Iterable[str] | None
) -> dict[str, dict]:
    key = TABLE_KEYS[table]
    query: dict = {"version": version}
    if ids is not None:
        query[key] = {"$in": list(ids)}
    return {doc[key]: doc for doc in database[table].find(query, projection={"_id": 0})}


def get_trips(
    database: Database, version: str, trip_ids: Iterable[str] | None = None
) -> dict[str, dict]:
    """trip_id -> trip info for one version (all trips if trip_ids is None)."""
    return _find(database, TRIPS, version, trip_ids)


def get_shapes(
    database: Database, version: str, shape_ids: Iterable[str] | None = None
) -> dict[str, dict]:
    """shape_id -> {points: [[lon, lat], ...], dist_m: [...]} for one version."""
    return _find(database, SHAPES, version, shape_ids)


def get_stops(
    database: Database, version: str, stop_ids: Iterable[str] | None = None
) -> dict[str, dict]:
    """stop_id -> {code, name, lat, lon} for one version."""
    return _find(database, STOPS, version, stop_ids)


def get_routes(
    database: Database, version: str, route_ids: Iterable[str] | None = None
) -> dict[str, dict]:
    """route_id -> {short_name, long_name, color} for one version."""
    return _find(database, ROUTES, version, route_ids)


def read_table(database: Database, version: str, filename: str) -> list[dict[str, str]]:
    """Rows of any file in a version's original zip, e.g. "stop_times.txt".

    Values are strings, as in the file. Loads the whole file, so prefer the
    lookups above for trips, shapes, stops and routes.
    """
    content = _files_bucket(database).get(version).read()
    return list(_rows(_read_zip(content), filename))

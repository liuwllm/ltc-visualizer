"""Parse LTC's GTFS-realtime protobufs and trim them down to what we store.

What is kept, and why:

* TripUpdates -- the raw feed is ~3 MB/poll: ~1,300 trips, most of them with no
  bus assigned yet, each carrying predictions for every remaining stop. We keep
  only trips that have a vehicle, collapse to the one *current* trip per
  vehicle (a bus often has its next trip assigned too), and keep only the
  *next* stop's arrival delay/time. Downstream stops are just extrapolations
  of that same delay. That is enough for schedule deviation, on-time/early/
  late percentages and active-vehicle counts.
* VehiclePositions -- TripUpdates has no location or speed, so average speed
  and distance-based bunching need this feed. It is tiny (~15 KB raw).
"""

from __future__ import annotations

from google.transit import gtfs_realtime_pb2 as gtfs

SKIPPED = gtfs.TripUpdate.StopTimeUpdate.SKIPPED
STALE_S = 120  # a prediction this far in the past means the stop was passed

# vehicle id -> (trip id, current stop sequence), from VehiclePositions
VehicleTrips = dict[str, tuple[str, int | None]]


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def parse_feed(raw: bytes) -> gtfs.FeedMessage:
    feed = gtfs.FeedMessage()
    feed.ParseFromString(raw)
    return feed


def _empty_feed_like(src: gtfs.FeedMessage) -> gtfs.FeedMessage:
    """A new feed with the same header and no entities."""
    out = gtfs.FeedMessage()
    out.header.gtfs_realtime_version = src.header.gtfs_realtime_version
    out.header.timestamp = src.header.timestamp
    return out


def _event(stu: gtfs.TripUpdate.StopTimeUpdate) -> gtfs.TripUpdate.StopTimeEvent:
    return stu.arrival if stu.HasField("arrival") else stu.departure


# --------------------------------------------------------------------------- #
# TripUpdates
# --------------------------------------------------------------------------- #

def _next_stop(
    tu: gtfs.TripUpdate, now: int, vp_seq: int | None
) -> gtfs.TripUpdate.StopTimeUpdate | None:
    """The stop the vehicle is heading to.

    LTC's feed can keep stops the bus already passed at the front of the list,
    and detoured stops come through as SKIPPED with no times. So: the first
    timed, non-skipped stop at/after VehiclePositions' current_stop_sequence,
    or, without a position, the first one whose prediction isn't stale.
    """
    for stu in tu.stop_time_update:
        if stu.schedule_relationship == SKIPPED or not _event(stu).time:
            continue
        if vp_seq is not None:
            if stu.stop_sequence >= vp_seq:
                return stu
        elif _event(stu).time >= now - STALE_S:
            return stu
    return None


def select_current_trips(
    src: gtfs.FeedMessage, vehicle_trips: VehicleTrips | None = None
) -> dict[str, tuple[gtfs.TripUpdate, gtfs.TripUpdate.StopTimeUpdate]]:
    """Map vehicle id -> (current TripUpdate, its next stop).

    The feed often assigns a vehicle both its current trip and its next one
    (and sometimes a just-finished one), so the VehiclePositions trip wins;
    without it, the trip with the soonest upcoming stop does.
    """
    now = src.header.timestamp
    vehicle_trips = vehicle_trips or {}

    # vehicle id -> (rank, TripUpdate, next stop); lowest rank wins
    best: dict[str, tuple[tuple, gtfs.TripUpdate, gtfs.TripUpdate.StopTimeUpdate]] = {}
    for ent in src.entity:
        if not ent.HasField("trip_update"):
            continue
        tu = ent.trip_update
        vid = tu.vehicle.id
        if not vid:
            continue
        vp_trip, vp_seq = vehicle_trips.get(vid, (None, None))
        is_vp_trip = tu.trip.trip_id == vp_trip
        stu = _next_stop(tu, now, vp_seq if is_vp_trip else None)
        if stu is None:
            continue
        rank = (not is_vp_trip, _event(stu).time)
        if vid not in best or rank < best[vid][0]:
            best[vid] = (rank, tu, stu)
    return {vid: (tu, stu) for vid, (_, tu, stu) in best.items()}


def _copy_trimmed_trip_update(
    dst: gtfs.TripUpdate,
    vid: str,
    tu: gtfs.TripUpdate,
    stu: gtfs.TripUpdate.StopTimeUpdate,
) -> None:
    trip = tu.trip
    dst.trip.trip_id = trip.trip_id
    dst.trip.route_id = trip.route_id
    dst.trip.start_date = trip.start_date
    dst.trip.start_time = trip.start_time
    if trip.HasField("direction_id"):
        dst.trip.direction_id = trip.direction_id
    if trip.schedule_relationship:
        dst.trip.schedule_relationship = trip.schedule_relationship
    dst.vehicle.id = vid
    if tu.timestamp:
        dst.timestamp = tu.timestamp

    # Next stop only. Scheduled time = time - delay, so it isn't stored.
    # LTC sometimes omits delay; leave it unset rather than store 0, so readers
    # can tell "unknown" apart from "on time" (check HasField("delay")).
    nxt = dst.stop_time_update.add()
    nxt.stop_sequence = stu.stop_sequence
    nxt.stop_id = stu.stop_id
    ev = _event(stu)
    dst_ev = nxt.arrival if stu.HasField("arrival") else nxt.departure
    if ev.HasField("delay"):
        dst_ev.delay = ev.delay
    dst_ev.time = ev.time


def trim_trip_updates(
    raw: bytes, vehicle_trips: VehicleTrips | None = None
) -> tuple[gtfs.FeedMessage, int]:
    """Return (trimmed feed, active vehicle count). See select_current_trips."""
    src = parse_feed(raw)
    current = select_current_trips(src, vehicle_trips)

    out = _empty_feed_like(src)
    for vid, (tu, stu) in current.items():
        ent = out.entity.add()
        ent.id = ""  # required by the proto; the vehicle id carries identity
        _copy_trimmed_trip_update(ent.trip_update, vid, tu, stu)
    return out, len(current)


# --------------------------------------------------------------------------- #
# VehiclePositions
# --------------------------------------------------------------------------- #

def _copy_trimmed_vehicle_position(
    dst: gtfs.VehiclePosition, vp: gtfs.VehiclePosition
) -> None:
    dst.vehicle.id = vp.vehicle.id
    if vp.HasField("trip"):
        dst.trip.trip_id = vp.trip.trip_id
        dst.trip.route_id = vp.trip.route_id
        if vp.trip.HasField("direction_id"):
            dst.trip.direction_id = vp.trip.direction_id
    dst.position.latitude = vp.position.latitude
    dst.position.longitude = vp.position.longitude
    if vp.position.HasField("bearing"):
        dst.position.bearing = vp.position.bearing
    if vp.position.HasField("speed"):
        dst.position.speed = vp.position.speed
    if vp.HasField("current_stop_sequence"):
        dst.current_stop_sequence = vp.current_stop_sequence
    if vp.HasField("current_status"):
        dst.current_status = vp.current_status
    if vp.timestamp:
        dst.timestamp = vp.timestamp


def trim_vehicle_positions(raw: bytes) -> gtfs.FeedMessage:
    """Keep only vehicles with a position, and only the fields we use."""
    src = parse_feed(raw)
    out = _empty_feed_like(src)
    for ent in src.entity:
        if not ent.HasField("vehicle") or not ent.vehicle.HasField("position"):
            continue
        dst = out.entity.add()
        dst.id = ""
        _copy_trimmed_vehicle_position(dst.vehicle, ent.vehicle)
    return out


def vehicle_trips(vp: gtfs.FeedMessage) -> VehicleTrips:
    """Map vehicle id -> (trip id, current stop sequence) for trim_trip_updates."""
    return {
        e.vehicle.vehicle.id: (
            e.vehicle.trip.trip_id,
            e.vehicle.current_stop_sequence
            if e.vehicle.HasField("current_stop_sequence")
            else None,
        )
        for e in vp.entity
    }

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


def _empty_feed_like(source: gtfs.FeedMessage) -> gtfs.FeedMessage:
    """A new feed with the same header and no entities."""
    feed = gtfs.FeedMessage()
    feed.header.gtfs_realtime_version = source.header.gtfs_realtime_version
    feed.header.timestamp = source.header.timestamp
    return feed


def _stop_event(
    stop_update: gtfs.TripUpdate.StopTimeUpdate,
) -> gtfs.TripUpdate.StopTimeEvent:
    return stop_update.arrival if stop_update.HasField("arrival") else stop_update.departure


# --------------------------------------------------------------------------- #
# TripUpdates
# --------------------------------------------------------------------------- #

def _has_left(stop_update: gtfs.TripUpdate.StopTimeUpdate, now: int) -> bool:
    """True if the bus left this stop more than STALE_S ago.

    Uses the departure time, so a bus holding at a timing point (arrived long
    ago, leaving soon) is still at that stop.
    """
    leaves = stop_update.departure if stop_update.departure.time else _stop_event(stop_update)
    return leaves.time < now - STALE_S


def _next_stop(
    trip_update: gtfs.TripUpdate, now: int, current_stop_sequence: int | None
) -> gtfs.TripUpdate.StopTimeUpdate | None:
    """The stop the vehicle is heading to.

    LTC's feed can keep stops the bus already passed at the front of the list,
    and detoured stops come through as SKIPPED with no times. So: the first
    timed, non-skipped stop the bus hasn't left, and not before
    VehiclePositions' current_stop_sequence when we have a reliable one. The
    VehiclePositions fetch can be up to a poll older than this feed, so its
    sequence is only a lower bound.
    """
    for stop_update in trip_update.stop_time_update:
        if stop_update.schedule_relationship == SKIPPED or not _stop_event(stop_update).time:
            continue
        if current_stop_sequence is not None and stop_update.stop_sequence < current_stop_sequence:
            continue
        if _has_left(stop_update, now):
            continue
        return stop_update
    return None


def select_current_trips(
    feed: gtfs.FeedMessage, vehicle_trips: VehicleTrips | None = None
) -> dict[str, tuple[gtfs.TripUpdate, gtfs.TripUpdate.StopTimeUpdate]]:
    """Map vehicle id -> (current TripUpdate, its next stop).

    The feed often assigns a vehicle both its current trip and its next one
    (and sometimes a just-finished one), so the VehiclePositions trip wins;
    without it, the trip with the soonest upcoming stop does.
    """
    now = feed.header.timestamp
    vehicle_trips = vehicle_trips or {}

    # vehicle id -> (rank, TripUpdate, next stop); lowest rank wins
    best_by_vehicle: dict[
        str, tuple[tuple, gtfs.TripUpdate, gtfs.TripUpdate.StopTimeUpdate]
    ] = {}
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        trip_update = entity.trip_update
        vehicle_id = trip_update.vehicle.id
        if not vehicle_id:
            continue
        position_trip_id, position_stop_sequence = vehicle_trips.get(
            vehicle_id, (None, None)
        )
        is_position_trip = trip_update.trip.trip_id == position_trip_id
        next_stop = _next_stop(
            trip_update, now, position_stop_sequence if is_position_trip else None
        )
        if next_stop is None:
            continue
        rank = (not is_position_trip, _stop_event(next_stop).time)
        if vehicle_id not in best_by_vehicle or rank < best_by_vehicle[vehicle_id][0]:
            best_by_vehicle[vehicle_id] = (rank, trip_update, next_stop)
    return {
        vehicle_id: (trip_update, next_stop)
        for vehicle_id, (_, trip_update, next_stop) in best_by_vehicle.items()
    }


def _copy_trimmed_trip_update(
    target: gtfs.TripUpdate,
    vehicle_id: str,
    source: gtfs.TripUpdate,
    next_stop: gtfs.TripUpdate.StopTimeUpdate,
) -> None:
    source_trip = source.trip
    target.trip.trip_id = source_trip.trip_id
    target.trip.route_id = source_trip.route_id
    target.trip.start_date = source_trip.start_date
    target.trip.start_time = source_trip.start_time
    if source_trip.HasField("direction_id"):
        target.trip.direction_id = source_trip.direction_id
    if source_trip.schedule_relationship:
        target.trip.schedule_relationship = source_trip.schedule_relationship
    target.vehicle.id = vehicle_id
    if source.timestamp:
        target.timestamp = source.timestamp

    # Next stop only, with both arrival and departure: at timing points a bus
    # can arrive late but leave on time, and on-time is usually measured on
    # departure. Scheduled time = time - delay, so it isn't stored.
    # LTC sometimes omits delay; leave it unset rather than store 0, so readers
    # can tell "unknown" apart from "on time" (check HasField("delay")).
    target_stop = target.stop_time_update.add()
    target_stop.stop_sequence = next_stop.stop_sequence
    target_stop.stop_id = next_stop.stop_id
    for event_name in ("arrival", "departure"):
        if not next_stop.HasField(event_name):
            continue
        source_event = getattr(next_stop, event_name)
        target_event = getattr(target_stop, event_name)
        if source_event.HasField("delay"):
            target_event.delay = source_event.delay
        if source_event.HasField("time"):
            target_event.time = source_event.time


def trim_trip_updates(
    raw: bytes, vehicle_trips: VehicleTrips | None = None
) -> tuple[gtfs.FeedMessage, int]:
    """Return (trimmed feed, active vehicle count). See select_current_trips."""
    feed = parse_feed(raw)
    current_trips = select_current_trips(feed, vehicle_trips)

    trimmed = _empty_feed_like(feed)
    for vehicle_id, (trip_update, next_stop) in current_trips.items():
        entity = trimmed.entity.add()
        entity.id = ""  # required by the proto; the vehicle id carries identity
        _copy_trimmed_trip_update(entity.trip_update, vehicle_id, trip_update, next_stop)
    return trimmed, len(current_trips)


# --------------------------------------------------------------------------- #
# VehiclePositions
# --------------------------------------------------------------------------- #

def _copy_trimmed_vehicle_position(
    target: gtfs.VehiclePosition, source: gtfs.VehiclePosition
) -> None:
    target.vehicle.id = source.vehicle.id
    if source.HasField("trip"):
        target.trip.trip_id = source.trip.trip_id
        target.trip.route_id = source.trip.route_id
        if source.trip.HasField("direction_id"):
            target.trip.direction_id = source.trip.direction_id
    target.position.latitude = source.position.latitude
    target.position.longitude = source.position.longitude
    if source.position.HasField("bearing"):
        target.position.bearing = source.position.bearing
    if source.position.HasField("speed"):
        target.position.speed = source.position.speed
    if source.HasField("current_stop_sequence"):
        target.current_stop_sequence = source.current_stop_sequence
    if source.HasField("current_status"):
        target.current_status = source.current_status
    if source.timestamp:
        target.timestamp = source.timestamp


def trim_vehicle_positions(raw: bytes) -> gtfs.FeedMessage:
    """Keep only vehicles with a position, and only the fields we use."""
    feed = parse_feed(raw)
    trimmed = _empty_feed_like(feed)
    for entity in feed.entity:
        if not entity.HasField("vehicle") or not entity.vehicle.HasField("position"):
            continue
        trimmed_entity = trimmed.entity.add()
        trimmed_entity.id = ""
        _copy_trimmed_vehicle_position(trimmed_entity.vehicle, entity.vehicle)
    return trimmed


def vehicle_trips(vehicle_positions: gtfs.FeedMessage) -> VehicleTrips:
    """Map vehicle id -> (trip id, current stop sequence) for trim_trip_updates.

    The stop sequence is only used when LTC also sends current_status: without
    it (most vehicles), current_stop_sequence is often several stops ahead of
    the bus, sometimes past the trip's last stop.
    """
    return {
        entity.vehicle.vehicle.id: (
            entity.vehicle.trip.trip_id,
            entity.vehicle.current_stop_sequence
            if entity.vehicle.HasField("current_stop_sequence")
            and entity.vehicle.HasField("current_status")
            else None,
        )
        for entity in vehicle_positions.entity
    }

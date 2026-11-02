// Dashboard metrics computed from stored snapshots.
//
// Vehicle data is stored as protobuf bytes, which Mongo can't aggregate, so
// documents are fetched by `ts` and decoded here. See the README's "Backend
// metrics" section for definitions.

import GtfsRealtimeBindings from "gtfs-realtime-bindings";

const { FeedMessage } = GtfsRealtimeBindings.transit_realtime;

const EARLY_S = -60; // more than 1 min early
const LATE_S = 300; // more than 5 min late
const BUNCH_DISTANCE_M = 250;
const MAX_VP_GAP_MS = 120_000; // matches the poller's VehiclePositions staleness rule

const has = (msg, field) => msg != null && Object.hasOwn(msg, field);
const normRoute = (id) => String(id ?? "").replace(/^0+(?=.)/, "");
const avg = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);
const round = (x, digits = 1) => (x == null ? null : Number(x.toFixed(digits)));

// Snapshot docs holding `field` in [from, to], or only the latest without a range.
async function findDocs(collection, field, from, to) {
  const filter = { [field]: { $exists: true } };
  if (from || to) {
    filter.ts = {};
    if (from) filter.ts.$gte = from;
    if (to) filter.ts.$lte = to;
    return collection.find(filter, { projection: { ts: 1, [field]: 1 } }).sort({ ts: 1 }).toArray();
  }
  return collection.find(filter, { projection: { ts: 1, [field]: 1 } }).sort({ ts: -1 }).limit(1).toArray();
}

const decode = (doc, field) => ({ ts: doc.ts, feed: FeedMessage.decode(doc[field].buffer) });

// [{ts, vehicles: [{id, routeId, tripId, startDate, directionId, stopSequence,
//   delay, speed, lat, lon}]}], one entry per TripUpdates snapshot, joined with
// the nearest VehiclePositions snapshot. Unknown values are undefined.
export async function loadSnapshots(db, from, to) {
  const [tuDocs, vpNew, vpOld] = await Promise.all([
    findDocs(db.collection("snapshots"), "tu", from, to),
    findDocs(db.collection("vehicle_positions"), "vp", from, to),
    findDocs(db.collection("snapshots"), "vp", from, to),
  ]);
  const vps = [...vpNew, ...vpOld].map((d) => decode(d, "vp"));

  return tuDocs.map((doc) => {
    const { ts, feed } = decode(doc, "tu");
    const nearest = vps.reduce(
      (best, vp) => (!best || Math.abs(vp.ts - ts) < Math.abs(best.ts - ts) ? vp : best),
      null,
    );
    const positions = new Map();
    if (nearest && Math.abs(nearest.ts - ts) <= MAX_VP_GAP_MS) {
      for (const { vehicle } of nearest.feed.entity) positions.set(vehicle.vehicle?.id, vehicle);
    }

    const vehicles = feed.entity.map(({ tripUpdate: tu }) => {
      const stop = tu.stopTimeUpdate[0];
      const event = has(stop?.departure, "delay") ? stop.departure : stop?.arrival;
      const vp = positions.get(tu.vehicle.id);
      return {
        id: tu.vehicle.id,
        routeId: tu.trip.routeId,
        tripId: tu.trip.tripId,
        startDate: tu.trip.startDate,
        directionId: has(tu.trip, "directionId") ? tu.trip.directionId : undefined,
        stopSequence: stop?.stopSequence,
        delay: has(event, "delay") ? event.delay : undefined,
        speed: has(vp?.position, "speed") ? vp.position.speed : undefined,
        lat: vp?.position.latitude,
        lon: vp?.position.longitude,
      };
    });
    return { ts, vehicles };
  });
}

// trip id -> static trip (direction_id, first_stop_sequence), using the newest
// static version covering each trip's service date (as static_gtfs.resolve_trips).
async function tripInfo(db, snapshots) {
  const tripsByDate = new Map();
  for (const v of snapshots.flatMap((s) => s.vehicles)) {
    if (!tripsByDate.has(v.startDate)) tripsByDate.set(v.startDate, new Set());
    tripsByDate.get(v.startDate).add(v.tripId);
  }
  const info = new Map();
  for (const [date, tripIds] of tripsByDate) {
    const versions = await db
      .collection("gtfs_versions")
      .find({ start_date: { $lte: date }, end_date: { $gte: date } })
      .sort({ first_seen_at: -1 })
      .toArray();
    for (const { _id: version } of versions) {
      const trips = await db
        .collection("gtfs_trips")
        .find({ version, trip_id: { $in: [...tripIds] } })
        .toArray();
      for (const trip of trips) {
        info.set(`${date}|${trip.trip_id}`, trip);
        tripIds.delete(trip.trip_id);
      }
    }
  }
  return info;
}

function haversineM(lat1, lon1, lat2, lon2) {
  const rad = Math.PI / 180;
  const a =
    Math.sin(((lat2 - lat1) * rad) / 2) ** 2 +
    Math.cos(lat1 * rad) * Math.cos(lat2 * rad) * Math.sin(((lon2 - lon1) * rad) / 2) ** 2;
  return 2 * 6_371_000 * Math.asin(Math.sqrt(a));
}

// Vehicles within BUNCH_DISTANCE_M of another on the same route and direction,
// excluding buses that haven't started their trip yet.
function countBunched(vehicles, trips) {
  const groups = new Map();
  for (const v of vehicles) {
    const trip = trips.get(`${v.startDate}|${v.tripId}`);
    const directionId = trip?.direction_id ?? v.directionId;
    const first = trip?.first_stop_sequence;
    if (v.lat == null || directionId == null) continue;
    if (first != null && v.stopSequence <= first) continue;
    const key = `${normRoute(v.routeId)}|${directionId}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(v);
  }
  let bunched = 0;
  for (const group of groups.values()) {
    bunched += group.filter((v) =>
      group.some((o) => o !== v && haversineM(v.lat, v.lon, o.lat, o.lon) <= BUNCH_DISTANCE_M),
    ).length;
  }
  return bunched;
}

const allVehicles = (snapshots) => snapshots.flatMap((s) => s.vehicles);
const delays = (snapshots) =>
  allVehicles(snapshots).filter((v) => v.delay !== undefined).map((v) => v.delay);

// metric -> (route-filtered snapshots, db) => values
export const METRICS = {
  speed(snapshots) {
    const speeds = allVehicles(snapshots).filter((v) => v.speed !== undefined).map((v) => v.speed * 3.6);
    return { avgSpeedKmh: round(avg(speeds)), samples: speeds.length };
  },

  deviation(snapshots) {
    const ds = delays(snapshots);
    return {
      avgDelaySec: round(avg(ds)),
      avgAbsDelaySec: round(avg(ds.map(Math.abs))),
      samples: ds.length,
    };
  },

  punctuality(snapshots) {
    const ds = delays(snapshots);
    const pct = (n) => (ds.length ? round((100 * n) / ds.length) : null);
    const early = ds.filter((d) => d < EARLY_S).length;
    const late = ds.filter((d) => d > LATE_S).length;
    return {
      earlyPct: pct(early),
      onTimePct: pct(ds.length - early - late),
      latePct: pct(late),
      samples: ds.length,
    };
  },

  async vehicles(snapshots, db) {
    const trips = await tripInfo(db, snapshots);
    return {
      active: round(avg(snapshots.map((s) => s.vehicles.length))),
      bunched: round(avg(snapshots.map((s) => countBunched(s.vehicles, trips)))),
    };
  },
};

// Keep only one route's vehicles (route ids match ignoring leading zeros).
export function filterRoute(snapshots, routeId) {
  if (routeId == null) return snapshots;
  const route = normRoute(routeId);
  return snapshots.map((s) => ({ ...s, vehicles: s.vehicles.filter((v) => normRoute(v.routeId) === route) }));
}

// Route ids seen in the snapshots (without leading zeros), in numeric order.
export const routeIds = (snapshots) =>
  [...new Set(allVehicles(snapshots).map((v) => normRoute(v.routeId)))].sort((a, b) => a - b || a.localeCompare(b));

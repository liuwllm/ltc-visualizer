import express from "express";
import { MongoClient } from "mongodb";
import { METRICS, filterRoute, loadSnapshots, routeIds } from "./metrics.js";

const MONGO_URI = process.env.MONGO_URI ?? "mongodb://localhost:27017";
const MONGO_DB = process.env.MONGO_DB ?? "ltc";
const PORT = Number(process.env.PORT ?? 3000);

// The driver reconnects on its own once connected; if Mongo is unreachable at
// startup, connect() throws and the container restarts.
const client = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 10_000 });
await client.connect();
const db = client.db(MONGO_DB);

const app = express();

app.get("/health", async (req, res) => {
  await db.command({ ping: 1 });
  res.json({ ok: true });
});

// Snapshots for ?from=&to= (the latest one without a range), or null after
// sending a 404/400.
async function load(req, res) {
  if (!Object.hasOwn(METRICS, req.params.metric)) {
    res.status(404).json({ error: `unknown metric ${req.params.metric}` });
    return null;
  }
  const [from, to] = [req.query.from, req.query.to].map((d) => (d ? new Date(d) : undefined));
  if ([from, to].some((d) => d && isNaN(d))) {
    res.status(400).json({ error: "invalid from/to date" });
    return null;
  }
  return loadSnapshots(db, from, to);
}

const range = (snapshots) => ({
  from: snapshots[0]?.ts ?? null,
  to: snapshots.at(-1)?.ts ?? null,
  snapshots: snapshots.length,
});

// GET /metrics/<metric>/routes[?from=ISO&to=ISO]: the metric for every route.
app.get("/metrics/:metric/routes", async (req, res) => {
  const snapshots = await load(req, res);
  if (!snapshots) return;
  const compute = METRICS[req.params.metric];
  const routes = await Promise.all(
    routeIds(snapshots).map(async (route) => ({ route, ...(await compute(filterRoute(snapshots, route), db)) })),
  );
  res.json({ metric: req.params.metric, ...range(snapshots), routes });
});

// GET /metrics/<metric>[/<routeId>][?from=ISO&to=ISO]; latest snapshot without a range.
app.get("/metrics/:metric{/:routeId}", async (req, res) => {
  const snapshots = await load(req, res);
  if (!snapshots) return;
  const { metric, routeId } = req.params;
  res.json({
    metric,
    route: routeId ?? null,
    ...range(snapshots),
    ...(await METRICS[metric](filterRoute(snapshots, routeId), db)),
  });
});

const server = app.listen(PORT, () => console.log(`Listening on port ${PORT}`));

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => server.close(() => client.close()));
}

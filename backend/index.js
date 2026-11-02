import express from "express";
import { MongoClient } from "mongodb";
import { METRICS, filterRoute, loadSnapshots } from "./metrics.js";

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

// GET /metrics/<metric>[/<routeId>][?from=ISO&to=ISO]; latest snapshot without a range.
app.get("/metrics/:metric{/:routeId}", async (req, res) => {
  const { metric, routeId } = req.params;
  if (!Object.hasOwn(METRICS, metric)) return res.status(404).json({ error: `unknown metric ${metric}` });
  const [from, to] = [req.query.from, req.query.to].map((d) => (d ? new Date(d) : undefined));
  if ([from, to].some((d) => d && isNaN(d))) return res.status(400).json({ error: "invalid from/to date" });

  const snapshots = await loadSnapshots(db, from, to);
  res.json({
    metric,
    route: routeId ?? null,
    from: snapshots[0]?.ts ?? null,
    to: snapshots.at(-1)?.ts ?? null,
    snapshots: snapshots.length,
    ...(await METRICS[metric](filterRoute(snapshots, routeId), db)),
  });
});

const server = app.listen(PORT, () => console.log(`Listening on port ${PORT}`));

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => server.close(() => client.close()));
}

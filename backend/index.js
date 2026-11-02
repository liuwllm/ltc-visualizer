import express from "express";
import { MongoClient } from "mongodb";

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

const server = app.listen(PORT, () => console.log(`Listening on port ${PORT}`));

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => server.close(() => client.close()));
}

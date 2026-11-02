import { Suspense } from "react";
import { connection } from "next/server";
import { MdCheckCircle, MdDirectionsBus, MdSchedule, MdSpeed } from "react-icons/md";
import StatCard from "../StatCard";

const BACKEND_URL = process.env.BACKEND_URL ?? "http://localhost:3000";
const NONE = "—";

async function getMetric(name) {
  const res = await fetch(`${BACKEND_URL}/metrics/${name}`);
  if (!res.ok) throw new Error(`${name}: HTTP ${res.status}`);
  return res.json();
}

// A value, or NONE if the metric failed to load or has no data.
const show = (m, format) => (m == null ? NONE : format(m));
const signed = (x) => (x > 0 ? `+${x}` : `${x}`);

async function Stats() {
  await connection(); // always render with the latest data
  const [speed, deviation, punctuality, vehicles] = (
    await Promise.allSettled(["speed", "deviation", "punctuality", "vehicles"].map(getMetric))
  ).map((r) => (r.status === "fulfilled" ? r.value : null));

  return (
    <div className="grid">
      <StatCard
        title="Average speed"
        icon={MdSpeed}
        value={show(speed?.avgSpeedKmh, (x) => `${x} km/h`)}
        detail={show(speed, (m) => `${m.samples} vehicles reporting`)}
      />
      <StatCard
        title="Schedule deviation"
        icon={MdSchedule}
        value={show(deviation?.avgDelaySec, (x) => `${signed(x)} s`)}
        detail={show(deviation?.avgAbsDelaySec, (x) => `avg absolute ${x} s`)}
      />
      <StatCard
        title="On-time"
        icon={MdCheckCircle}
        value={show(punctuality?.onTimePct, (x) => `${x}%`)}
        detail={show(punctuality?.earlyPct, () => `${punctuality.earlyPct}% early · ${punctuality.latePct}% late`)}
      />
      <StatCard
        title="Active vehicles"
        icon={MdDirectionsBus}
        value={show(vehicles?.active, String)}
        detail={show(vehicles?.bunched, (x) => `${x} bunched`)}
      />
    </div>
  );
}

export default function MetricsPage() {
  return (
    <>
      <h1 className="page-title">Metrics</h1>
      <Suspense fallback={<div className="grid-loading">Loading…</div>}>
        <Stats />
      </Suspense>
    </>
  );
}

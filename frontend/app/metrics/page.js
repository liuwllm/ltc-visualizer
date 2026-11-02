import { Suspense } from "react";
import { connection } from "next/server";
import StatCard from "../StatCard";
import { CARDS, getMetrics } from "./cards";

async function Stats() {
  await connection(); // always render with the latest data
  const results = await Promise.all(Object.keys(CARDS).map(getMetrics));
  return (
    <div className="grid">
      {Object.entries(CARDS).map(([metric, card], i) => (
        <StatCard
          key={metric}
          title={card.title}
          icon={card.icon}
          value={card.value(results[i])}
          detail={card.detail(results[i])}
          href={`/metrics/${metric}`}
        />
      ))}
    </div>
  );
}

export default function MetricsPage() {
  return (
    <>
      <h1 className="page-title">Metrics</h1>
      <Suspense fallback={<div className="muted">Loading…</div>}>
        <Stats />
      </Suspense>
    </>
  );
}

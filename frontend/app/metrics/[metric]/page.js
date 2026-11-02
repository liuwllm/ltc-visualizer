import { Suspense } from "react";
import Link from "next/link";
import { notFound } from "next/navigation";
import { MdArrowDownward, MdArrowUpward } from "react-icons/md";
import StatCard from "../../StatCard";
import { CARDS, getMetrics } from "../cards";

export function generateStaticParams() {
  return Object.keys(CARDS).map((metric) => ({ metric }));
}

// ?sort=<field>&dir=asc|desc (default: the card's first sort field, highest first)
async function RouteStats({ metric, searchParams }) {
  const card = CARDS[metric];
  const query = await searchParams;
  const sort = card.sorts.find((s) => s.key === query.sort) ?? card.sorts[0];
  const dir = query.dir === "asc" ? "asc" : "desc";

  const [summary, byRoute] = await Promise.all([getMetrics(metric), getMetrics(`${metric}/routes`)]);
  const sign = dir === "asc" ? 1 : -1;
  const routes = (byRoute?.routes ?? []).toSorted(
    (a, b) => (a[sort.key] == null) - (b[sort.key] == null) || sign * (a[sort.key] - b[sort.key]),
  );

  return (
    <>
      <StatCard title={card.title} icon={card.icon} value={card.value(summary)} detail={card.detail(summary)} />

      <div className="toolbar">
        <h2>By route</h2>
        {card.sorts.length > 1 &&
          card.sorts.map((s) => (
            <Link key={s.key} href={{ query: { sort: s.key, dir } }} className={`pill${s === sort ? " active" : ""}`}>
              {s.label}
            </Link>
          ))}
        <Link href={{ query: { sort: sort.key, dir: dir === "asc" ? "desc" : "asc" } }} className="pill">
          {dir === "asc" ? <MdArrowUpward /> : <MdArrowDownward />}
          {dir === "asc" ? "Lowest first" : "Highest first"}
        </Link>
      </div>

      {routes.length ? (
        <div className="grid">
          {routes.map((r) => (
            <StatCard
              key={r.route}
              title={`Route ${r.route}`}
              icon={card.icon}
              value={(sort.value ?? card.value)(r)}
              detail={(sort.detail ?? card.detail)(r)}
            />
          ))}
        </div>
      ) : (
        <p className="muted">No route data.</p>
      )}
    </>
  );
}

export default async function MetricPage({ params, searchParams }) {
  const { metric } = await params;
  if (!Object.hasOwn(CARDS, metric)) notFound();
  return (
    <>
      <h1 className="page-title">
        <Link href="/metrics" className="crumb">Metrics</Link> / {CARDS[metric].title}
      </h1>
      <Suspense fallback={<div className="muted">Loading…</div>}>
        <RouteStats metric={metric} searchParams={searchParams} />
      </Suspense>
    </>
  );
}

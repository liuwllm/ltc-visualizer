import { MdCheckCircle, MdDirectionsBus, MdSchedule, MdSpeed } from "react-icons/md";

const BACKEND_URL = process.env.BACKEND_URL ?? "http://localhost:3000";

// Backend JSON for a /metrics path, or null if it couldn't be loaded.
export async function getMetrics(path) {
  try {
    const res = await fetch(`${BACKEND_URL}/metrics/${path}`);
    return res.ok ? await res.json() : null;
  } catch {
    return null;
  }
}

const fmt = (x, format) => (x == null ? "—" : format(x));

// metric -> card content for one backend result (null if it failed), and the
// fields its per-route cards can be sorted by (the first is the default). A
// sort can override the route cards' value/detail so they show what's sorted.
export const CARDS = {
  speed: {
    title: "Average speed",
    icon: MdSpeed,
    value: (m) => fmt(m?.avgSpeedKmh, (x) => `${x} km/h`),
    detail: (m) => m && `${m.samples} vehicles reporting`,
    sorts: [{ key: "avgSpeedKmh", label: "Speed" }],
  },
  deviation: {
    title: "Schedule deviation",
    icon: MdSchedule,
    value: (m) => fmt(m?.avgAbsDelaySec, (x) => `${x} s`),
    detail: (m) => m?.avgDelaySec != null && `net ${m.avgDelaySec > 0 ? "+" : ""}${m.avgDelaySec} s (+ = late)`,
    sorts: [{ key: "avgAbsDelaySec", label: "Deviation" }],
  },
  punctuality: {
    title: "On-time",
    icon: MdCheckCircle,
    value: (m) => fmt(m?.onTimePct, (x) => `${x}%`),
    detail: (m) => m?.samples > 0 && `${m.earlyPct}% early · ${m.latePct}% late`,
    sorts: [
      { key: "onTimePct", label: "On time" },
      {
        key: "latePct",
        label: "Late",
        value: (m) => fmt(m.latePct, (x) => `${x}% late`),
        detail: (m) => m.samples > 0 && `${m.onTimePct}% on time · ${m.earlyPct}% early`,
      },
      {
        key: "earlyPct",
        label: "Early",
        value: (m) => fmt(m.earlyPct, (x) => `${x}% early`),
        detail: (m) => m.samples > 0 && `${m.onTimePct}% on time · ${m.latePct}% late`,
      },
    ],
  },
  vehicles: {
    title: "Active vehicles",
    icon: MdDirectionsBus,
    value: (m) => fmt(m?.active, String),
    detail: (m) => m?.bunched != null && `${m.bunched} bunched`,
    sorts: [
      { key: "active", label: "Active" },
      {
        key: "bunched",
        label: "Bunched",
        value: (m) => fmt(m.bunched, (x) => `${x} bunched`),
        detail: (m) => `${m.active} active`,
      },
    ],
  },
};

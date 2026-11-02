"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { MdShowChart, MdStar } from "react-icons/md";

const LINKS = [{ href: "/metrics", label: "Metrics", icon: MdShowChart }];

export default function Sidebar() {
  const pathname = usePathname();
  return (
    <nav className="sidebar">
      <div className="brand">
        <MdStar className="brand-icon" />
        Polaris
      </div>
      {LINKS.map(({ href, label, icon: Icon }) => (
        <Link
          key={href}
          href={href}
          className={`nav-link${pathname.startsWith(href) ? " active" : ""}`}
        >
          <Icon />
          {label}
        </Link>
      ))}
    </nav>
  );
}

import Link from "next/link";
import { MdArrowForward } from "react-icons/md";

export default function StatCard({ title, icon: Icon, value, detail, href }) {
  return (
    <div className="card">
      <div className="card-head">
        <span className="card-icon">
          <Icon />
        </span>
        {title}
        {href && (
          <Link href={href} className="card-link" aria-label={`${title} by route`}>
            <MdArrowForward />
          </Link>
        )}
      </div>
      <div className="card-value">{value}</div>
      {detail && <div className="card-detail">{detail}</div>}
    </div>
  );
}

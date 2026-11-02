export default function StatCard({ title, icon: Icon, value, detail }) {
  return (
    <div className="card">
      <div className="card-head">
        <span className="card-icon">
          <Icon />
        </span>
        {title}
      </div>
      <div className="card-value">{value}</div>
      {detail && <div className="card-detail">{detail}</div>}
    </div>
  );
}

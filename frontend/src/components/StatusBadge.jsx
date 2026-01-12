export default function StatusBadge({ status }) {
  const map = {
    live: "success",
    cached: "warning",
    running: "info",
  };

  return (
    <span className={`badge badge-${map[status] || "default"}`}>
      {status || "—"}
    </span>
  );
}

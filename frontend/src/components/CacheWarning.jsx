export default function CacheWarning({ status }) {
  if (status?.masterlist_source !== "cache") return null;

  return (
    <div style={{
      background: "#fff3cd",
      color: "#856404",
      padding: "10px 16px",
      borderBottom: "1px solid #ffeeba",
      fontSize: "14px"
    }}>
      ⚠️ Data is currently served from cached sources due to upstream unavailability.
      Metrics may not reflect the latest NAV updates.
    </div>
  );
}

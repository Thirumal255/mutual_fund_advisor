import { useEffect, useState } from "react";
import Login from "./pages/Login";
import Metrics from "./pages/Metrics";
import Admin from "./pages/Admin";

/* ===============================
   Status helpers
   =============================== */
function statusStyle(status) {
  if (status === "live") return { color: "#155724", fontWeight: "bold" };
  if (status === "cached") return { color: "#856404", fontWeight: "bold" };
  if (status === "running") return { color: "#0c5460", fontWeight: "bold" };
  return { color: "#383d41" };
}

function statusLabel(status) {
  if (status === "live") return "✅ Live";
  if (status === "cached") return "⚠️ Cached";
  if (status === "running") return "⏳ Running";
  return "Unknown";
}

/* ===============================
   System Status Table
   =============================== */
function SystemStatusTable({ status, role, onRefresh }) {
  if (!status) return null;

  const rows = [
    { name: "Masterlist", data: status.masterlist },
    { name: "Metrics", data: status.metrics },
    { name: "SID Extraction", data: status.sid_extraction },
    { name: "UI Payload", data: status.ui_payload },
  ];

  return (
    <div style={{ marginBottom: 24 }}>
      {/* Header row */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 8,
        }}
      >
        <h3 style={{ margin: 0 }}>System Status</h3>

        {role === "admin" && (
          <Admin
            compact
            onStatusUpdate={onRefresh}
          />
        )}
      </div>

      <table
        style={{
          width: "100%",
          borderCollapse: "collapse",
          fontSize: "14px",
        }}
      >
        <thead>
          <tr style={{ background: "#f1f1f1" }}>
            <th style={th}>Module</th>
            <th style={th}>Status</th>
            <th style={th}>Last Updated</th>
            <th style={th}>Message</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.name}>
              <td style={td}><b>{row.name}</b></td>
              <td style={{ ...td, ...statusStyle(row.data?.status) }}>
                {statusLabel(row.data?.status)}
              </td>
              <td style={td}>
                {row.data?.last_updated
                  ? new Date(row.data.last_updated).toLocaleString()
                  : "N/A"}
              </td>
              <td style={td}>{row.data?.message || "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

const th = {
  border: "1px solid #ccc",
  padding: "8px",
  textAlign: "left",
};

const td = {
  border: "1px solid #ddd",
  padding: "8px",
};

/* ===============================
   App
   =============================== */
export default function App() {
  const [token, setToken] = useState(localStorage.getItem("token"));
  const [role, setRole] = useState(localStorage.getItem("role"));
  const [systemStatus, setSystemStatus] = useState(null);

  const refreshSystemStatus = async () => {
    if (!token) return;
    try {
      const res = await fetch("http://127.0.0.1:8000/api/status", {
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = await res.json();
      setSystemStatus(data);
    } catch {
      // ignore
    }
  };

  useEffect(() => {
    refreshSystemStatus();
  }, [token]);

  if (!token) {
    return (
      <Login
        onLogin={(t, r) => {
          localStorage.setItem("token", t);
          localStorage.setItem("role", r);
          setToken(t);
          setRole(r);
        }}
      />
    );
  }

  return (
    <div style={{ padding: 16 }}>
      {/* Top bar */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 16,
        }}
      >
        <h2 style={{ margin: 0 }}>Mutual Fund Advisor</h2>

        <button
          onClick={() => {
            localStorage.clear();
            location.reload();
          }}
        >
          Logout
        </button>
      </div>

      <SystemStatusTable
        status={systemStatus}
        role={role}
        onRefresh={refreshSystemStatus}
      />

      <Metrics token={token} />
    </div>
  );
}

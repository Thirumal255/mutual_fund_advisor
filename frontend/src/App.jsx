import { useEffect, useState, useRef } from "react";
import Login from "./pages/Login";
import Metrics from "./pages/Metrics";
import Admin from "./pages/Admin";

/* ===============================
   Status helpers
   =============================== */
function statusStyle(status) {
  if (status === "live") return { color: "#155724", fontWeight: 600 };
  if (status === "cached") return { color: "#856404", fontWeight: 600 };
  if (status === "running") return { color: "#0c5460", fontWeight: 600 };
  return { color: "#444" };
}

function statusLabel(status) {
  if (status === "live") return "✅ Live";
  if (status === "cached") return "⚠️ Cached";
  if (status === "running") return "⏳ Running";
  return "—";
}

/* ===============================
   System Status Table
   =============================== */
function SystemStatusTable({ status, role, token, onRefresh }) {
  if (!status) return null;

  const rows = [
    { name: "Masterlist", data: status.masterlist },
    { name: "Metrics", data: status.metrics },
    { name: "SID Extraction", data: status.sid_extraction },
    { name: "UI Payload", data: status.ui_payload },
  ];

  return (
    <div style={card}>
      <div style={cardHeader}>
        <h3 style={{ margin: 0 }}>📊 System Status</h3>

        {role === "admin" && (
          <Admin compact token={token} onStatusUpdate={onRefresh} />
        )}
      </div>

      <table style={table}>
        <thead>
          <tr>
            <th style={th}>Module</th>
            <th style={th}>Status</th>
            <th style={th}>Last Updated</th>
            <th style={th}>Message</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.name}>
              <td style={td}><b>{r.name}</b></td>
              <td style={{ ...td, ...statusStyle(r.data?.status) }}>
                {statusLabel(r.data?.status)}
              </td>
              <td style={td}>
                {r.data?.last_updated
                  ? new Date(r.data.last_updated).toLocaleString()
                  : "N/A"}
              </td>
              <td style={td}>{r.data?.message || "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* ===============================
   Chat Drawer
   =============================== */
function ChatDrawer({ token, onClose }) {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);

  const sessionIdRef = useRef(
    `session_${Date.now()}_${Math.random().toString(36).slice(2)}`
  );

  useEffect(() => {
    setMessages([
      {
        role: "assistant",
        text:
          "👋 Hi! I’m your Mutual Fund Advisor.\n\nTell me your goal, time horizon, and risk comfort — I’ll take care of the rest.",
      },
    ]);
  }, []);

  const sendMessage = async () => {
    if (!input.trim() || loading) return;

    const userText = input;
    setInput("");
    setMessages((m) => [...m, { role: "user", text: userText }]);
    setLoading(true);

    try {
      const res = await fetch("http://127.0.0.1:8000/api/chat", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          message: userText,
          session_id: sessionIdRef.current,
        }),
      });

      const data = await res.json();
      setMessages((m) => [
        ...m,
        { role: "assistant", text: data?.message || "⚠️ Please try again." },
      ]);
    } catch {
      setMessages((m) => [
        ...m,
        { role: "assistant", text: "⚠️ Chat service unavailable." },
      ]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={chatDrawer}>
      <div style={chatHeader}>
        <b>💬 Mutual Fund Advisor</b>
        <button onClick={onClose} style={iconButton}>✖</button>
      </div>

      <div style={chatBody}>
        {messages.map((m, i) => (
          <div
            key={i}
            style={{
              ...bubble,
              alignSelf: m.role === "user" ? "flex-end" : "flex-start",
              background:
                m.role === "user" ? "#d1e7ff" : "#f1f3f5",
            }}
          >
            <div style={{ whiteSpace: "pre-wrap" }}>{m.text}</div>
          </div>
        ))}
        {loading && <div style={{ opacity: 0.6 }}>⏳ Thinking…</div>}
      </div>

      <div style={chatInput}>
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && sendMessage()}
          placeholder="Ask about mutual funds…"
          style={chatInputBox}
        />
      </div>
    </div>
  );
}

/* ===============================
   App
   =============================== */
export default function App() {
  const [token, setToken] = useState(localStorage.getItem("token"));
  const [role, setRole] = useState(localStorage.getItem("role"));
  const [systemStatus, setSystemStatus] = useState(null);
  const [showChat, setShowChat] = useState(false);

  const refreshSystemStatus = async () => {
    if (!token) return;
    const res = await fetch("http://127.0.0.1:8000/api/status", {
      headers: { Authorization: `Bearer ${token}` },
    });
    setSystemStatus(await res.json());
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
    <div style={page}>
      <div style={topBar}>
        <h2 style={{ margin: 0 }}>💼 Mutual Fund Advisor</h2>
        <div>
          <button style={primaryBtn} onClick={() => setShowChat(true)}>
            💬 Chat
          </button>
          <button
            style={secondaryBtn}
            onClick={() => {
              localStorage.clear();
              location.reload();
            }}
          >
            Logout
          </button>
        </div>
      </div>

      <SystemStatusTable
        status={systemStatus}
        role={role}
        token={token}
        onRefresh={refreshSystemStatus}
      />

      <Metrics token={token} />

      {showChat && (
        <ChatDrawer token={token} onClose={() => setShowChat(false)} />
      )}
    </div>
  );
}

/* ===============================
   Theme Styles
   =============================== */
const page = {
  minHeight: "100vh",
  padding: 24,
  background: "linear-gradient(135deg, #f8f9fa, #eef2f7)",
  fontFamily: "Inter, system-ui, sans-serif",
};

const topBar = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  marginBottom: 24,
};

const card = {
  background: "#fff",
  borderRadius: 12,
  padding: 16,
  marginBottom: 24,
  boxShadow: "0 8px 24px rgba(0,0,0,0.05)",
};

const cardHeader = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  marginBottom: 12,
};

const table = {
  width: "100%",
  borderCollapse: "collapse",
};

const th = {
  textAlign: "left",
  padding: 10,
  background: "#f1f3f5",
  borderBottom: "1px solid #ddd",
};

const td = {
  padding: 10,
  borderBottom: "1px solid #eee",
};

const primaryBtn = {
  padding: "8px 14px",
  marginRight: 8,
  borderRadius: 8,
  border: "none",
  background: "#2563eb",
  color: "#fff",
  cursor: "pointer",
};

const secondaryBtn = {
  padding: "8px 14px",
  borderRadius: 8,
  border: "1px solid #ccc",
  background: "#fff",
  cursor: "pointer",
};

/* Chat */
const chatDrawer = {
  position: "fixed",
  top: 0,
  right: 0,
  width: 420,
  height: "100vh",
  background: "#fff",
  display: "flex",
  flexDirection: "column",
  boxShadow: "-8px 0 24px rgba(0,0,0,0.1)",
  zIndex: 1000,
};

const chatHeader = {
  padding: 16,
  borderBottom: "1px solid #eee",
  display: "flex",
  justifyContent: "space-between",
};

const chatBody = {
  flex: 1,
  padding: 16,
  overflowY: "auto",
  display: "flex",
  flexDirection: "column",
  gap: 8,
};

const bubble = {
  maxWidth: "85%",
  padding: 10,
  borderRadius: 10,
};

const chatInput = {
  padding: 12,
  borderTop: "1px solid #eee",
};

const chatInputBox = {
  width: "100%",
  padding: 10,
  borderRadius: 8,
  border: "1px solid #ccc",
};

const iconButton = {
  background: "transparent",
  border: "none",
  cursor: "pointer",
  fontSize: 16,
};

import { useEffect, useState, useRef } from "react";

import Login from "./pages/Login";
import Metrics from "./pages/Metrics";
import Admin from "./pages/Admin";

import Card from "./components/Card";
import Button from "./components/Button";

import ChatMessage from "./components/ChatMessage";
import ChatInput from "./components/ChatInput";

/* ===============================
   Status helpers
   =============================== */
function statusStyle(status) {
  if (status === "live") return { color: "#16a34a", fontWeight: 600 };
  if (status === "cached") return { color: "#f59e0b", fontWeight: 600 };
  if (status === "running") return { color: "#0284c7", fontWeight: 600 };
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
    <Card
      title={
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            width: "100%",
          }}
        >
          <span>📊 System Status</span>
          {role === "admin" && (
            <Admin compact token={token} onStatusUpdate={onRefresh} />
          )}
        </div>
      }
    >
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            <th style={{ padding: 10, background: "#f1f5f9" }}>Module</th>
            <th style={{ padding: 10, background: "#f1f5f9" }}>Status</th>
            <th style={{ padding: 10, background: "#f1f5f9" }}>
              Last Updated
            </th>
            <th style={{ padding: 10, background: "#f1f5f9" }}>Message</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.name}>
              <td style={{ padding: 10 }}>
                <b>{r.name}</b>
              </td>
              <td style={{ padding: 10, ...statusStyle(r.data?.status) }}>
                {statusLabel(r.data?.status)}
              </td>
              <td style={{ padding: 10 }}>
                {r.data?.last_updated
                  ? new Date(r.data.last_updated).toLocaleString()
                  : "N/A"}
              </td>
              <td style={{ padding: 10 }}>
                {r.data?.message || "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </Card>
  );
}

/* ===============================
   Chat Overlay
   =============================== */
function ChatOverlay({ token, onClose }) {
  const bottomRef = useRef(null);

  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [profile, setProfile] = useState({});

  const sessionIdRef = useRef(
    `session_${Date.now()}_${Math.random().toString(36).slice(2)}`
  );

  useEffect(() => {
    setMessages([
      {
        role: "assistant",
        text:
          "👋 Hi! I’m your Mutual Fund Advisor.\n\nTell me your goal, time horizon, and risk comfort — I’ll take care of the rest.",
        ts: Date.now(),
      },
    ]);
  }, []);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  function extractProfileFromText(text) {
    const updated = {};

    if (/sip/i.test(text)) updated.investmentType = "SIP";
    if (/lump/i.test(text)) updated.investmentType = "Lumpsum";
    if (/moderate/i.test(text)) updated.risk = "Moderate";
    if (/aggressive/i.test(text)) updated.risk = "Aggressive";
    if (/conservative/i.test(text)) updated.risk = "Conservative";
    if (/retirement/i.test(text)) updated.goal = "Retirement";
    if (/wealth/i.test(text)) updated.goal = "Wealth Creation";
    if (/year/i.test(text)) updated.horizon = "5–7 years";

    return updated;
  }

  const sendMessage = async () => {
    if (!input.trim() || loading) return;

    const userText = input;
    setInput("");

    setMessages((m) => [
      ...m,
      { role: "user", text: userText, ts: Date.now() },
    ]);

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
      console.log("CHAT RESPONSE:", data);

      const aiText =
        data?.message ||
        data?.reply ||
        data?.response ||
        "⚠️ Try again.";


      setMessages((m) => [
  ...m,
  { role: "assistant", text: aiText, ts: Date.now() },
]);

// ✅ Prefer backend profile if present, else fallback
if (data?.profile && typeof data.profile === "object") {
  setProfile(data.profile);
} else {
  setProfile((p) => ({
    ...p,
    ...extractProfileFromText(aiText),
  }));
}

    } catch {
      setMessages((m) => [
        ...m,
        {
          role: "assistant",
          text: "⚠️ Chat service unavailable.",
          ts: Date.now(),
        },
      ]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(15, 23, 42, 0.25)",
        zIndex: 999999,
        display: "flex",
        justifyContent: "flex-end",
      }}
    >
      <div
        style={{
          width: 420,
          height: "100vh",
          background: "#ffffff",
          display: "flex",
          flexDirection: "column",
          boxShadow: "-8px 0 24px rgba(0,0,0,0.25)",
        }}
      >
        {/* Header */}
        <div
          style={{
            padding: "14px 16px",
            borderBottom: "1px solid #e5e7eb",
            background: "#f8fafc",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <div>
            <div style={{ fontWeight: 600 }}>💬 Mutual Fund Advisor</div>
            <div style={{ fontSize: 12, color: "#64748b" }}>
              AI-powered investment guidance
            </div>
          </div>
          <button
            onClick={onClose}
            style={{
              border: "none",
              background: "transparent",
              fontSize: 18,
              cursor: "pointer",
              color: "#475569",
            }}
          >
            ✕
          </button>
        </div>

        {/* Advisor Profile Summary */}
        {Object.keys(profile).length > 0 && (
          <div
            style={{
              margin: 12,
              padding: 12,
              background: "#eef2ff",
              borderRadius: 10,
              fontSize: 13,
              border: "1px solid #c7d2fe",
            }}
          >
            <div style={{ fontWeight: 600, marginBottom: 6 }}>
              🧠 Advisor Understanding
            </div>

            {Object.entries(profile).map(([key, value]) => (
  <div key={key}>
    <b>{key.replace(/_/g, " ").toUpperCase()}:</b>{" "}
    {String(value)}
  </div>
))}
          </div>
        )}

        {/* Messages */}
        <div
          style={{
            flex: 1,
            padding: 16,
            overflowY: "auto",
            display: "flex",
            flexDirection: "column",
            gap: 10,
            background: "#f9fafb",
          }}
        >
          {messages.map((m, i) => (
            <ChatMessage
              key={i}
              role={m.role}
              text={m.text}
              ts={m.ts}
            />
          ))}

          {loading && (
            <div
              style={{
                alignSelf: "flex-start",
                fontSize: 13,
                color: "#64748b",
              }}
            >
              ⏳ Advisor is thinking…
            </div>
          )}

          <div ref={bottomRef} />
        </div>

        {/* Input */}
        <ChatInput
          value={input}
          onChange={setInput}
          onSend={sendMessage}
          loading={loading}
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

  useEffect(() => {
    document.body.style.overflow = showChat ? "hidden" : "auto";
    return () => (document.body.style.overflow = "auto");
  }, [showChat]);

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
    <>
      <div style={{ padding: 24 }}>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: 24,
          }}
        >
          <h2>💼 Mutual Fund Advisor</h2>
          <div>
            <Button onClick={() => setShowChat(true)}>💬 Chat</Button>{" "}
            <Button
              variant="secondary"
              onClick={() => {
                localStorage.clear();
                location.reload();
              }}
            >
              Logout
            </Button>
          </div>
        </div>

        <SystemStatusTable
          status={systemStatus}
          role={role}
          token={token}
          onRefresh={refreshSystemStatus}
        />

        <Card title="📈 Mutual Fund Metrics">
          <Metrics token={token} />
        </Card>
      </div>

      {showChat && (
        <ChatOverlay token={token} onClose={() => setShowChat(false)} />
      )}
    </>
  );
}

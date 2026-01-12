export default function ChatMessage({ role, text, ts }) {
  const isUser = role === "user";

  return (
    <div
      style={{
        alignSelf: isUser ? "flex-end" : "flex-start",
        maxWidth: "80%",
        background: isUser ? "#dbeafe" : "#ffffff",
        border: isUser ? "none" : "1px solid #e5e7eb",
        padding: "10px 12px",
        borderRadius: 12,
        lineHeight: 1.4,
        fontSize: 14,
        whiteSpace: "pre-wrap",
      }}
    >
      {text}

      <div
        style={{
          marginTop: 4,
          fontSize: 11,
          color: "#64748b",
          textAlign: isUser ? "right" : "left",
        }}
      >
        {new Date(ts).toLocaleTimeString([], {
          hour: "2-digit",
          minute: "2-digit",
        })}
      </div>
    </div>
  );
}

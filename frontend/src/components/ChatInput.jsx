export default function ChatInput({
  value,
  onChange,
  onSend,
  loading,
}) {
  return (
    <div
      style={{
        padding: 12,
        borderTop: "1px solid #e5e7eb",
        background: "#ffffff",
      }}
    >
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && onSend()}
        placeholder="Ask about goals, SIPs, risk, returns…"
        disabled={loading}
        style={{
          width: "100%",
          padding: "10px 12px",
          borderRadius: 8,
          border: "1px solid #cbd5f5",
          outline: "none",
          fontSize: 14,
        }}
      />
    </div>
  );
}

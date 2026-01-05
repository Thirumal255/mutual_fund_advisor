export default function ChatMessage({ message }) {
  const isUser = message.role === "user";

  let text = "";

  if (isUser) {
    text = message.content;
  } else {
    text = message.content.message || JSON.stringify(message.content, null, 2);
  }

  return (
    <div
      style={{
        ...styles.bubble,
        alignSelf: isUser ? "flex-end" : "flex-start",
        background: isUser ? "#DCF8C6" : "#fff"
      }}
    >
      {text}
    </div>
  );
}

const styles = {
  bubble: {
    maxWidth: "70%",
    padding: "10px 14px",
    borderRadius: "8px",
    marginBottom: "10px",
    boxShadow: "0 1px 3px rgba(0,0,0,0.1)",
    whiteSpace: "pre-wrap"
  }
};

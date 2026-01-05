import { useState } from "react";

export default function ChatInput({ onSend }) {
  const [text, setText] = useState("");

  const handleSend = () => {
    onSend(text);
    setText("");
  };

  return (
    <div style={styles.container}>
      <input
        style={styles.input}
        value={text}
        placeholder="Ask about mutual funds..."
        onChange={(e) => setText(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && handleSend()}
      />
      <button style={styles.button} onClick={handleSend}>
        Send
      </button>
    </div>
  );
}

const styles = {
  container: {
    display: "flex",
    padding: "12px",
    borderTop: "1px solid #ddd"
  },
  input: {
    flex: 1,
    padding: "10px",
    fontSize: "14px"
  },
  button: {
    marginLeft: "8px",
    padding: "0 16px"
  }
};

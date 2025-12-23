import { useState } from "react";
import { login } from "../api/api";

export default function Login({ onLogin }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");

  async function submit(e) {
    e.preventDefault();
    try {
      const data = await login(username, password);
      onLogin(data.access_token, data.role);
    } catch (err) {
      setError(err.message);
    }
  }

  return (
    <div style={{ maxWidth: 400, margin: "80px auto" }}>
      <h3>Login</h3>
      <form onSubmit={submit}>
        <input placeholder="Username" onChange={e => setUsername(e.target.value)} />
        <br /><br />
        <input type="password" placeholder="Password" onChange={e => setPassword(e.target.value)} />
        <br /><br />
        <button>Login</button>
      </form>
      {error && <p style={{ color: "red" }}>{error}</p>}
      <p>Admin: admin / adminpass</p>
      <p>User: user / userpass</p>
    </div>
  );
}

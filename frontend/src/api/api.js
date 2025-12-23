const API = "http://127.0.0.1:8000/api";

export async function login(username, password) {
  const body = new URLSearchParams();
  body.append("username", username);
  body.append("password", password);

  const res = await fetch(`${API}/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  });

  if (!res.ok) {
    const e = await res.json();
    throw new Error(e.detail || "Login failed");
  }
  return res.json();
}

export async function getMetrics(token) {
  const res = await fetch(`${API}/metrics`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) throw new Error("Failed to load metrics");
  return res.json();
}

export async function adminAction(token, path) {
  const res = await fetch(`${API}/admin/${path}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.json();
}

export async function getStatus(token) {
  const res = await fetch("http://127.0.0.1:8000/api/status", {
    headers: {
      Authorization: `Bearer ${token}`
    }
  });
  return res.json();
}

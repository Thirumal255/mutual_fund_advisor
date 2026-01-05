const API = "http://127.0.0.1:8000/api";

/* ============================
   🆕 Helper: auth header
   ============================ */
function authHeaders(token) {
  if (!token) {
    throw new Error("Not authenticated");
  }
  return {
    Authorization: `Bearer ${token}`,
  };
}

/* ============================
   🆕 Helper: handle 401
   ============================ */
async function handleUnauthorized(res) {
  if (res.status === 401) {
    // Clear invalid auth state
    localStorage.removeItem("token");
    localStorage.removeItem("role");
    throw new Error("Invalid authentication");
  }
}

/* ============================
   Login (unchanged)
   ============================ */
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

/* ============================
   Metrics
   ============================ */
export async function getMetrics(token) {
  const res = await fetch(`${API}/metrics`, {
    headers: authHeaders(token),
  });

  await handleUnauthorized(res);

  if (!res.ok) {
    const e = await res.json();
    throw new Error(e.detail || "Failed to load metrics");
  }

  return res.json();
}

/* ============================
   Admin actions
   ============================ */
export async function adminAction(token, path) {
  const res = await fetch(`${API}/admin/${path}`, {
    method: "POST",
    headers: {
      ...authHeaders(token),
      "Content-Type": "application/json",
    },
  });

  await handleUnauthorized(res);

  if (!res.ok) {
    const e = await res.json();
    throw new Error(e.detail || "Admin action failed");
  }

  return res.json();
}

/* ============================
   Status
   ============================ */
export async function getStatus(token) {
  const res = await fetch(`${API}/status`, {
    headers: authHeaders(token),
  });

  await handleUnauthorized(res);

  if (!res.ok) {
    const e = await res.json();
    throw new Error(e.detail || "Failed to load status");
  }

  return res.json();
}

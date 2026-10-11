const TOKEN_KEY = "flashcore.token";

// Backend origin, e.g. https://1-2-3-4.sslip.io, set at build time (Vercel). Empty = same origin:
// local Docker, where nginx serves this app and proxies /api and /ws.
const API_URL = (import.meta.env.VITE_API_URL || "").replace(/\/+$/, "");

function readStoredToken() {
  try {
    return sessionStorage.getItem(TOKEN_KEY);
  } catch {
    return null;
  }
}

let token = readStoredToken();

export function getToken() {
  return token;
}

export function setToken(value) {
  token = value;
  try {
    if (value) sessionStorage.setItem(TOKEN_KEY, value);
    else sessionStorage.removeItem(TOKEN_KEY);
  } catch {
    // storage unavailable (private mode): keep it in memory only
  }
}

export function tokenExpired() {
  if (!token) return true;
  try {
    const payload = JSON.parse(atob(token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/")));
    return payload.exp * 1000 < Date.now();
  } catch {
    return true;
  }
}

export class ApiError extends Error {
  constructor(status, code) {
    super(code || `HTTP ${status}`);
    this.status = status;
    this.code = code;
  }
}

async function request(path, { method = "GET", body, auth = false } = {}) {
  const headers = { "Content-Type": "application/json" };
  if (auth && token) headers.Authorization = `Bearer ${token}`;
  const res = await fetch(API_URL + path, { method, headers, body: body ? JSON.stringify(body) : undefined });
  let data = null;
  try {
    data = await res.json();
  } catch {
    // empty or non-JSON body
  }
  if (!res.ok) throw new ApiError(res.status, data?.error || data?.detail);
  return data;
}

export async function login(username, password) {
  const data = await request("/api/auth/token/", { method: "POST", body: { username, password } });
  setToken(data.access);
}

export const listSales = () => request("/api/sales/");
export const getSale = (id) => request(`/api/sales/${id}/`);
export const myOrders = () => request("/api/orders/mine/", { auth: true });
export const buy = (id) => request(`/api/sales/${id}/buy`, { method: "POST", auth: true });

export function wsUrl(saleId) {
  const base = API_URL ? new URL(API_URL) : window.location;
  const proto = base.protocol === "https:" ? "wss" : "ws";
  return `${proto}://${base.host}/ws?token=${encodeURIComponent(token)}&sale_id=${saleId}`;
}

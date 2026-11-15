import { useState } from "react";
import { login } from "../api.js";

// Set at build time for the public demo (deploy/deploy_frontend.ps1); visitors can't sign up.
const DEMO_LOGIN = import.meta.env.VITE_DEMO_LOGIN;

export default function Login({ onLoggedIn }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function onSubmit(event) {
    event.preventDefault();
    if (!username || !password) {
      setError("Enter your username and password.");
      return;
    }
    setBusy(true);
    setError("");
    try {
      await login(username, password);
      onLoggedIn();
    } catch (err) {
      if (err.status === 401) setError("Wrong username or password.");
      else if (err.status === 429) setError("Too many login attempts. Wait a few seconds and try again.");
      else setError("Could not log in. Is the server running?");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="login">
      <form className="card" onSubmit={onSubmit}>
        <h1>FlashCore</h1>
        <p className="muted">Log in to join the flash sale.</p>
        {DEMO_LOGIN && <p className="muted">Demo accounts: {DEMO_LOGIN}</p>}
        <label>
          Username
          <input value={username} onChange={(e) => setUsername(e.target.value)} autoComplete="username" />
        </label>
        <label>
          Password
          <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="current-password" />
        </label>
        {error && <p className="error">{error}</p>}
        <button className="primary" disabled={busy}>{busy ? "Logging in…" : "Log in"}</button>
      </form>
    </div>
  );
}

import { useCallback, useEffect, useState } from "react";
import { getToken, setToken, tokenExpired } from "./api.js";
import Login from "./pages/Login.jsx";
import SaleList from "./pages/SaleList.jsx";
import SalePage from "./pages/SalePage.jsx";

function useHash() {
  const [hash, setHash] = useState(window.location.hash);
  useEffect(() => {
    const onChange = () => setHash(window.location.hash);
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  return hash;
}

export default function App() {
  const [authed, setAuthed] = useState(() => Boolean(getToken()) && !tokenExpired());
  const hash = useHash();

  const logout = useCallback(() => {
    setToken(null);
    setAuthed(false);
  }, []);

  if (!authed) return <Login onLoggedIn={() => setAuthed(true)} />;

  const match = hash.match(/^#\/sales\/(\d+)$/);
  return (
    <div className="shell">
      <header className="topbar">
        <a href="#/" className="brand">FlashCore</a>
        <button className="link" onClick={logout}>Log out</button>
      </header>
      <main>
        {match ? <SalePage saleId={Number(match[1])} onLogout={logout} /> : <SaleList />}
      </main>
    </div>
  );
}

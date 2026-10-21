import { useCallback, useEffect, useState } from "react";
import { buy, getSale, myOrders } from "../api.js";
import { useSaleSocket } from "../useSaleSocket.js";

const STATUS_TEXT = {
  RESERVED: "Reserved — confirming your payment…",
  PENDING: "Reserved — confirming your payment…",
  CONFIRMED: "Order confirmed. It's yours!",
  FAILED: "Payment failed — the item went back on sale. You can try again.",
};

const FINAL = new Set(["CONFIRMED", "FAILED"]);

// Updates arrive from three places (buy response, WebSocket, order reload) in any order:
// never let a late, older one move the same order back from CONFIRMED/FAILED.
function advance(current, next) {
  if (current && current.id === next.id && FINAL.has(current.status) && !FINAL.has(next.status)) return current;
  return next;
}

function useNow() {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);
  return now;
}

function countdown(ms) {
  const s = Math.max(0, Math.floor(ms / 1000));
  const pad = (n) => String(n).padStart(2, "0");
  return `${pad(Math.floor(s / 3600))}:${pad(Math.floor((s % 3600) / 60))}:${pad(s % 60)}`;
}

export default function SalePage({ saleId, onLogout }) {
  const [sale, setSale] = useState(null);
  const [remaining, setRemaining] = useState(null);
  const [order, setOrder] = useState(null); // { id, status }
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState("");
  const now = useNow();

  const restoreOrder = useCallback(async () => {
    try {
      const orders = await myOrders(); // newest first
      const mine = orders.find((o) => o.sale === saleId);
      if (mine) setOrder((current) => advance(current, { id: mine.id, status: mine.status }));
    } catch (err) {
      if (err.status === 401) onLogout();
    }
  }, [saleId, onLogout]);

  useEffect(() => {
    getSale(saleId)
      .then((s) => {
        setSale(s);
        setRemaining((current) => current ?? s.remaining);
      })
      .catch(() => setNotice("Could not load this sale."));
    restoreOrder();
  }, [saleId, restoreOrder]);

  const connected = useSaleSocket(saleId, (msg) => {
    if (msg.type === "stock" && msg.sale_id === saleId) setRemaining(msg.remaining);
    else if (msg.type === "order" && msg.sale_id === saleId) {
      setOrder((current) => advance(current, { id: msg.order_id, status: msg.status }));
    }
    else if (msg.type === "auth_error") onLogout();
  });

  // Order updates sent while the socket was down are not replayed: reload after every reconnect.
  useEffect(() => {
    if (connected) restoreOrder();
  }, [connected, restoreOrder]);

  async function onBuy() {
    setBusy(true);
    setNotice("");
    try {
      const result = await buy(saleId);
      // A WebSocket update may already have arrived for this order; don't move it backwards.
      setOrder((current) => advance(current, { id: result.order_id, status: "RESERVED" }));
    } catch (err) {
      if (err.status === 401) onLogout();
      else if (err.code === "sold_out") setNotice("Sold out (or the sale hasn't started).");
      else if (err.code === "already_purchased") {
        setNotice("You already have one of these.");
        restoreOrder();
      } else if (err.status === 503) setNotice("We're very busy right now — please try again.");
      else setNotice("Something went wrong. Please try again.");
    } finally {
      setBusy(false);
    }
  }

  if (!sale) return <p className="muted">{notice || "Loading…"}</p>;

  const startsIn = new Date(sale.starts_at).getTime() - now;
  const ended = new Date(sale.ends_at).getTime() < now;
  const soldOut = remaining === 0;
  const holding = order && order.status !== "FAILED";
  const disabled = busy || soldOut || startsIn > 0 || ended || holding;

  let buttonText = "Buy now";
  if (busy) buttonText = "Buying…";
  else if (holding) buttonText = "You're in";
  else if (startsIn > 0) buttonText = `Starts in ${countdown(startsIn)}`;
  else if (ended) buttonText = "Sale ended";
  else if (soldOut) buttonText = "Sold out";

  return (
    <div className="card sale">
      <div className="sale-head">
        <h1>{sale.product_name}</h1>
        <span className={`dot ${connected ? "on" : "off"}`} title={connected ? "Live" : "Reconnecting…"} />
      </div>
      <p className="price">₹{sale.price}</p>

      <div className="stock">
        <span className="stock-number">{remaining ?? "–"}</span>
        <span className="muted"> of {sale.total_stock} left</span>
      </div>
      <div className="bar">
        <div style={{ width: `${sale.total_stock ? ((remaining ?? 0) / sale.total_stock) * 100 : 0}%` }} />
      </div>

      <button className="primary big" onClick={onBuy} disabled={disabled}>{buttonText}</button>

      {order && <p className={`status status-${order.status.toLowerCase()}`}>{STATUS_TEXT[order.status]}</p>}
      {notice && <p className="notice">{notice}</p>}
      <a href="#/" className="muted back">← All sales</a>
    </div>
  );
}

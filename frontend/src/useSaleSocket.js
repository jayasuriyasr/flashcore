import { useEffect, useRef, useState } from "react";
import { wsUrl } from "./api.js";

/** Live stock + order updates for one sale. Reconnects with backoff (1s, 2s, 4s … 10s). */
export function useSaleSocket(saleId, onMessage) {
  const [connected, setConnected] = useState(false);
  const handler = useRef(onMessage);
  handler.current = onMessage;

  useEffect(() => {
    let socket;
    let timer;
    let attempt = 0;
    let closedByUs = false;

    const open = () => {
      socket = new WebSocket(wsUrl(saleId));
      socket.onopen = () => {
        attempt = 0;
        setConnected(true);
      };
      socket.onmessage = (event) => {
        try {
          handler.current(JSON.parse(event.data));
        } catch {
          // ignore malformed frames
        }
      };
      socket.onclose = (event) => {
        setConnected(false);
        if (closedByUs) return;
        if (event.code === 4401) {
          handler.current({ type: "auth_error" });
          return;
        }
        const delay = Math.min(1000 * 2 ** attempt, 10000);
        attempt += 1;
        timer = setTimeout(open, delay);
      };
    };

    open();
    return () => {
      closedByUs = true;
      clearTimeout(timer);
      socket?.close();
    };
  }, [saleId]);

  return connected;
}

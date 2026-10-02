import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// `npm run dev` proxies API and WebSocket calls to the running stack behind nginx (port 80).
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": "http://localhost",
      "/ws": { target: "ws://localhost", ws: true },
    },
  },
});

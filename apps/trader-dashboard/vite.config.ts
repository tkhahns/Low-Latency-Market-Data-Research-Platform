import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/live": "http://localhost:8000",
      "/research": "http://localhost:8000",
      "/symbols": "http://localhost:8000",
      "/latest": "http://localhost:8000",
      "/freshness": "http://localhost:8000",
      "/alerts": "http://localhost:8000",
      "/health": "http://localhost:8000",
      "/obsidian": "http://localhost:8000",
      "/ws": {
        target: "ws://localhost:8000",
        ws: true,
      },
    },
  },
  build: {
    outDir: "dist",
  },
});

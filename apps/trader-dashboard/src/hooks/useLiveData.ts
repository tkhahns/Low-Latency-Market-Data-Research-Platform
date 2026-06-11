import { useEffect, useRef, useState } from "react";
import type { ConnectionStatus, LivePayload } from "../types";

const apiKey = new URLSearchParams(window.location.search).get("api_key");
const snapshotUrl = apiKey
  ? `/live/snapshot?api_key=${encodeURIComponent(apiKey)}`
  : "/live/snapshot";

export function useLiveData() {
  const [payload, setPayload] = useState<LivePayload | null>(null);
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const pollingRef = useRef(false);
  const wsRef = useRef<WebSocket | null>(null);

  function startPolling() {
    if (pollingRef.current) return;
    pollingRef.current = true;

    async function poll() {
      try {
        const res = await fetch(snapshotUrl);
        if (res.ok) {
          setStatus("polling");
          setPayload(await res.json());
        } else {
          setStatus("disconnected");
        }
      } catch {
        setStatus("disconnected");
      }
      setTimeout(poll, 1000);
    }

    poll();
  }

  useEffect(() => {
    function connect() {
      const protocol = window.location.protocol === "https:" ? "wss" : "ws";
      const wsUrl = apiKey
        ? `${protocol}://${window.location.host}/ws/live?api_key=${encodeURIComponent(apiKey)}`
        : `${protocol}://${window.location.host}/ws/live`;

      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;
      let opened = false;

      ws.onopen = () => {
        opened = true;
        setStatus("live");
      };

      ws.onmessage = (event) => {
        setPayload(JSON.parse(event.data));
      };

      ws.onclose = () => {
        setStatus("disconnected");
        if (opened) {
          setTimeout(connect, 1000);
        } else {
          startPolling();
        }
      };

      ws.onerror = () => {
        ws.close();
      };
    }

    connect();

    return () => {
      wsRef.current?.close();
    };
  }, []);

  return { payload, status };
}

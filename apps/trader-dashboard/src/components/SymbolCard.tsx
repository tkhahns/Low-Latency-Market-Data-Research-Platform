import { useEffect, useRef, useState } from "react";
import type { ResearchInsight, SymbolSnapshot } from "../types";
import { ResearchPanel } from "./ResearchPanel";

function fmt(value: number | undefined | null): string {
  if (value === undefined || value === null) return "-";
  return Number(value).toFixed(2);
}

function useFlash(value: number | undefined): "flash-up" | "flash-down" | "" {
  const prev = useRef<number | undefined>(undefined);
  const [cls, setCls] = useState<"flash-up" | "flash-down" | "">("");

  useEffect(() => {
    if (value !== undefined && prev.current !== undefined && value !== prev.current) {
      const next = value > prev.current ? "flash-up" : "flash-down";
      setCls(next);
      const timer = setTimeout(() => setCls(""), 650);
      prev.current = value;
      return () => clearTimeout(timer);
    }
    prev.current = value;
  }, [value]);

  return cls;
}

interface Props {
  snapshot: SymbolSnapshot;
  research: ResearchInsight[];
}

export function SymbolCard({ snapshot, research }: Props) {
  const top = snapshot.top_of_book ?? {};
  const bar = snapshot.bar_1s ?? {};
  const metrics = snapshot.metrics ?? {};
  const fresh = snapshot.freshness ?? {};
  const alert = snapshot.alerts?.[0];
  const status = fresh.status ?? "pending";

  const bidFlash = useFlash(top.bid_price);
  const askFlash = useFlash(top.ask_price);

  const statusCls =
    status === "stale"
      ? "bg-stale-bg text-stale-text"
      : "bg-fresh-bg text-fresh-text";

  return (
    <article className="rounded-lg bg-surface border border-border p-3.5">
      <div className="flex items-start justify-between gap-3 mb-3.5">
        <div>
          <div className="text-[22px] font-bold">{snapshot.symbol}</div>
          <p className="text-sm text-muted mt-1">{top.exchange ?? "Waiting for feed"}</p>
        </div>
        <span className={`rounded-full px-2 py-1 text-xs ${statusCls}`}>{status}</span>
      </div>

      <div className={`flex justify-between border-t border-border-inner py-2 ${bidFlash}`}>
        <span className="text-xs text-muted">Bid</span>
        <span className="tabular-nums text-right">
          {fmt(top.bid_price)} × {top.bid_size ?? "-"}
        </span>
      </div>

      <div className={`flex justify-between border-t border-border-inner py-2 ${askFlash}`}>
        <span className="text-xs text-muted">Ask</span>
        <span className="tabular-nums text-right">
          {fmt(top.ask_price)} × {top.ask_size ?? "-"}
        </span>
      </div>

      <div className="flex justify-between border-t border-border-inner py-2">
        <span className="text-xs text-muted">Spread</span>
        <span className="tabular-nums text-right">{fmt(top.spread)}</span>
      </div>

      <div className="flex justify-between border-t border-border-inner py-2">
        <span className="text-xs text-muted">Volume</span>
        <span className="tabular-nums text-right">{bar.volume ?? "-"}</span>
      </div>

      <div className="flex justify-between border-t border-border-inner py-2">
        <span className="text-xs text-muted">1s VWAP</span>
        <span className="tabular-nums text-right">{fmt(bar.vwap)}</span>
      </div>

      <div className="flex justify-between border-t border-border-inner py-2">
        <span className="text-xs text-muted">Volatility</span>
        <span className="tabular-nums text-right">{metrics.volatility_bps ?? "-"} bps</span>
      </div>

      <div className="flex justify-between border-t border-border-inner py-2">
        <span className="text-xs text-muted">Freshness</span>
        <span className="tabular-nums text-right">{fresh.freshness_lag_ms ?? "-"} ms</span>
      </div>

      {alert && (
        <div className="mt-3 text-[13px] text-alert">
          {alert.severity}: {alert.message}
        </div>
      )}

      <ResearchPanel insights={research} />
    </article>
  );
}

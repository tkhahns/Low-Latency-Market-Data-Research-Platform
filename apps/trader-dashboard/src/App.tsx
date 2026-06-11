import { useEffect, useState } from "react";
import { ConnectionBadge } from "./components/ConnectionBadge";
import { ResearchDigest } from "./components/ResearchDigest";
import { SymbolCard } from "./components/SymbolCard";
import { useLiveData } from "./hooks/useLiveData";
import { useResearch } from "./hooks/useResearch";

type Tab = "watchlist" | "research";

export default function App() {
  const { payload, status } = useLiveData();
  const { bySymbol, digest } = useResearch();
  const [tab, setTab] = useState<Tab>("watchlist");
  const [obsidianUri, setObsidianUri] = useState<string | null>(null);

  useEffect(() => {
    fetch("/obsidian/project")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => d && setObsidianUri(d.obsidian_uri))
      .catch(() => null);
  }, []);

  const symbolCount = payload?.symbols.length ?? 0;
  const researchCount = digest.length;

  return (
    <div className="min-h-screen bg-bg text-text font-sans">
      <div className="w-full max-w-[1180px] mx-auto px-4 py-7">
        {/* Top bar */}
        <div className="flex items-center justify-between gap-6 flex-wrap mb-5">
          <div>
            <h1 className="text-[26px] font-bold m-0">Market Data</h1>
            <p className="text-sm text-muted mt-1.5">Real-time crypto market data platform</p>
          </div>

          <div className="flex items-center gap-3 flex-wrap">
            <div className="min-w-[96px] rounded-lg bg-surface border border-border px-3 py-2.5">
              <span className="text-xs text-muted block">Symbols</span>
              <strong className="text-[22px] block mt-1">{symbolCount}</strong>
            </div>

            <ConnectionBadge status={status} />

            {obsidianUri && (
              <a
                href={obsidianUri}
                className="inline-flex min-h-[66px] items-center px-3.5 rounded-lg bg-surface border border-border text-[#dfe6e2] text-[13px] font-semibold no-underline hover:border-[#3e4a45] hover:bg-[#1c2220] transition-colors"
              >
                Open in Obsidian
              </a>
            )}
          </div>
        </div>

        {/* Tab nav */}
        <div className="flex gap-1 mb-5 border-b border-border">
          <TabButton active={tab === "watchlist"} onClick={() => setTab("watchlist")}>
            Watchlist
          </TabButton>
          <TabButton active={tab === "research"} onClick={() => setTab("research")}>
            Research
            {researchCount > 0 && (
              <span className="ml-1.5 bg-research-count-bg text-research-count rounded-full px-1.5 py-0.5 text-[10px]">
                {researchCount}
              </span>
            )}
          </TabButton>
        </div>

        {/* Watchlist tab */}
        {tab === "watchlist" && (
          <>
            {!payload ? (
              <div className="text-center text-muted py-16 text-sm">
                Connecting to feed…
              </div>
            ) : (
              <div className="grid gap-3" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))" }}>
                {payload.symbols.map((snap) => (
                  <SymbolCard
                    key={snap.symbol}
                    snapshot={snap}
                    research={bySymbol[snap.symbol] ?? []}
                  />
                ))}
              </div>
            )}
          </>
        )}

        {/* Research tab */}
        {tab === "research" && <ResearchDigest insights={digest} />}
      </div>
    </div>
  );
}

function TabButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors flex items-center -mb-px ${
        active
          ? "border-fresh-text text-fresh-text"
          : "border-transparent text-muted hover:text-text"
      }`}
    >
      {children}
    </button>
  );
}

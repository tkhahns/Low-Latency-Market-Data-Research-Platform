import { useState } from "react";
import type { ResearchInsight, Sentiment } from "../types";

const sentimentStyles: Record<Sentiment, string> = {
  bullish: "bg-bullish-bg text-bullish-text",
  bearish: "bg-bearish-bg text-bearish-text",
  neutral: "bg-neutral-bg text-neutral-text",
};

interface Props {
  insights: ResearchInsight[];
}

export function ResearchPanel({ insights }: Props) {
  const [open, setOpen] = useState(false);
  const top = insights.slice(0, 3);
  if (!top.length) return null;

  return (
    <div className="mt-3 border-t border-border-inner pt-2">
      <button
        onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-2 text-xs text-muted cursor-pointer select-none w-full text-left"
      >
        <span>Research</span>
        <span className="bg-research-count-bg text-research-count rounded-full px-1.5 py-0.5 text-[11px]">
          {top.length}
        </span>
        <span className="ml-auto">{open ? "▲" : "▼"}</span>
      </button>

      {open && (
        <div className="mt-1 space-y-2">
          {top.map((i) => (
            <div
              key={i.doc_id}
              className="rounded-md bg-research-bg border border-research-border p-2"
            >
              <div className="flex items-start justify-between gap-2 mb-1">
                <a
                  href={i.source_uri}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-link text-[12px] font-semibold flex-1 min-w-0 truncate hover:underline"
                >
                  {i.title}
                </a>
                <span
                  className={`text-[10px] rounded px-1 py-0.5 flex-shrink-0 ${sentimentStyles[i.sentiment]}`}
                >
                  {i.sentiment}
                </span>
              </div>
              <p className="text-[11px] text-muted-2 leading-relaxed line-clamp-2 m-0">
                {i.summary}
              </p>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

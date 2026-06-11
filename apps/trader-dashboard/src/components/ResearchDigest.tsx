import type { ResearchInsight, Sentiment } from "../types";

const sentimentStyles: Record<Sentiment, string> = {
  bullish: "bg-bullish-bg text-bullish-text",
  bearish: "bg-bearish-bg text-bearish-text",
  neutral: "bg-neutral-bg text-neutral-text",
};

interface Props {
  insights: ResearchInsight[];
}

export function ResearchDigest({ insights }: Props) {
  if (!insights.length) {
    return (
      <div className="text-center text-muted py-16 text-sm">
        No research insights yet. The ingestor polls every 15 minutes.
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {insights.map((i) => (
        <div
          key={i.doc_id}
          className="rounded-lg bg-surface border border-border p-4"
        >
          <div className="flex items-start justify-between gap-3 mb-2">
            <a
              href={i.source_uri}
              target="_blank"
              rel="noopener noreferrer"
              className="text-link font-semibold text-sm hover:underline flex-1 min-w-0"
            >
              {i.title}
            </a>
            <span
              className={`text-[10px] rounded px-1.5 py-0.5 flex-shrink-0 ${sentimentStyles[i.sentiment]}`}
            >
              {i.sentiment}
            </span>
          </div>

          <p className="text-[13px] text-muted-2 leading-relaxed mb-3">{i.summary}</p>

          <div className="flex flex-wrap gap-2 text-[11px]">
            {i.symbols.map((sym) => (
              <span
                key={sym}
                className="bg-research-count-bg text-research-count rounded px-1.5 py-0.5"
              >
                {sym}
              </span>
            ))}
            {i.tags.map((tag) => (
              <span
                key={tag}
                className="bg-neutral-bg text-neutral-text rounded px-1.5 py-0.5"
              >
                {tag}
              </span>
            ))}
          </div>

          {i.entities.length > 0 && (
            <div className="mt-2 text-[11px] text-muted">
              Entities: {i.entities.join(", ")}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

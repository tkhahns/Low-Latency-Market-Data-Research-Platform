import { useEffect, useState } from "react";
import type { ResearchDigestPayload, ResearchInsight } from "../types";

const apiKey = new URLSearchParams(window.location.search).get("api_key");
const researchUrl = apiKey
  ? `/research/digest?api_key=${encodeURIComponent(apiKey)}`
  : "/research/digest";

export function useResearch() {
  const [bySymbol, setBySymbol] = useState<Record<string, ResearchInsight[]>>({});
  const [digest, setDigest] = useState<ResearchInsight[]>([]);

  useEffect(() => {
    async function poll() {
      try {
        const res = await fetch(researchUrl);
        if (res.ok) {
          const data: ResearchDigestPayload = await res.json();
          const insights = data.insights ?? [];
          setDigest(insights);

          const grouped: Record<string, ResearchInsight[]> = {};
          for (const insight of insights) {
            for (const sym of insight.symbols ?? []) {
              if (!grouped[sym]) grouped[sym] = [];
              grouped[sym].push(insight);
            }
          }
          setBySymbol(grouped);
        }
      } catch {
        // non-fatal
      }
      setTimeout(poll, 60_000);
    }

    poll();
  }, []);

  return { bySymbol, digest };
}

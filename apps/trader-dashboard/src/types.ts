export interface TopOfBook {
  exchange?: string;
  bid_price?: number;
  bid_size?: number;
  ask_price?: number;
  ask_size?: number;
  spread?: number;
}

export interface Bar1s {
  volume?: number;
  vwap?: number;
}

export interface Metrics {
  volatility_bps?: number;
}

export interface Freshness {
  status?: "fresh" | "stale" | "pending";
  freshness_lag_ms?: number;
}

export interface Alert {
  severity: string;
  message: string;
}

export interface SymbolSnapshot {
  symbol: string;
  top_of_book?: TopOfBook;
  bar_1s?: Bar1s;
  metrics?: Metrics;
  freshness?: Freshness;
  alerts?: Alert[];
}

export interface LivePayload {
  symbols: SymbolSnapshot[];
}

export type Sentiment = "bullish" | "bearish" | "neutral";

export interface ResearchInsight {
  doc_id: string;
  title: string;
  source_uri: string;
  summary: string;
  sentiment: Sentiment;
  symbols: string[];
  tags: string[];
  entities: string[];
  extractor: string;
  published_time?: string;
}

export interface ResearchDigestPayload {
  insights: ResearchInsight[];
}

export type ConnectionStatus = "connecting" | "live" | "polling" | "disconnected";

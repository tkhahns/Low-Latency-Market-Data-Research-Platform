const watchlist = document.querySelector("#watchlist");
const connection = document.querySelector("#connection");
const symbolCount = document.querySelector("#symbol-count");
const obsidianLink = document.querySelector("#obsidian-link");

// Research panel state: symbol → insight[]
let researchBySymbol = {};

function money(value) {
  if (value === undefined || value === null) return "-";
  return Number(value).toFixed(2);
}

function sentimentClass(s) {
  if (s === "bullish") return "sentiment-bullish";
  if (s === "bearish") return "sentiment-bearish";
  return "sentiment-neutral";
}

function renderResearch(symbol) {
  const insights = (researchBySymbol[symbol] || []).slice(0, 3);
  if (!insights.length) return "";
  const items = insights.map(i => `
    <div class="research-item">
      <div class="research-title">
        <a href="${i.source_uri}" target="_blank" rel="noopener">${i.title}</a>
        <span class="sentiment ${sentimentClass(i.sentiment)}">${i.sentiment}</span>
      </div>
      <div class="research-summary">${i.summary}</div>
    </div>
  `).join("");
  return `
    <details class="research-panel">
      <summary>Research <span class="research-count">${insights.length}</span></summary>
      ${items}
    </details>
  `;
}

function render(snapshot) {
  const top = snapshot.top_of_book || {};
  const bar = snapshot.bar_1s || {};
  const metrics = snapshot.metrics || {};
  const fresh = snapshot.freshness || {};
  const alert = (snapshot.alerts || [])[0];
  const status = fresh.status || "pending";
  return `
    <article class="card">
      <div class="card-header">
        <div>
          <div class="symbol">${snapshot.symbol}</div>
          <p>${top.exchange || "Waiting for feed"}</p>
        </div>
        <span class="status ${status === "stale" ? "stale" : ""}">${status}</span>
      </div>
      <div class="row"><span class="label">Bid</span><span class="value">${money(top.bid_price)} x ${top.bid_size ?? "-"}</span></div>
      <div class="row"><span class="label">Ask</span><span class="value">${money(top.ask_price)} x ${top.ask_size ?? "-"}</span></div>
      <div class="row"><span class="label">Spread</span><span class="value">${money(top.spread)}</span></div>
      <div class="row"><span class="label">Volume</span><span class="value">${bar.volume ?? "-"}</span></div>
      <div class="row"><span class="label">1s VWAP</span><span class="value">${money(bar.vwap)}</span></div>
      <div class="row"><span class="label">Volatility</span><span class="value">${metrics.volatility_bps ?? "-"} bps</span></div>
      <div class="row"><span class="label">Freshness</span><span class="value">${fresh.freshness_lag_ms ?? "-"} ms</span></div>
      ${alert ? `<div class="alerts">${alert.severity}: ${alert.message}</div>` : ""}
      ${renderResearch(snapshot.symbol)}
    </article>
  `;
}

async function loadObsidianProject() {
  if (!obsidianLink) return;
  try {
    const response = await fetch("/obsidian/project");
    if (!response.ok) return;
    const project = await response.json();
    obsidianLink.href = project.obsidian_uri;
    obsidianLink.title = project.vault_path;
  } catch {
    obsidianLink.href = "/obsidian/project";
  }
}

function renderFrame(payload) {
  symbolCount.textContent = payload.symbols.length;
  watchlist.innerHTML = payload.symbols.map(render).join("");
}

const apiKey = new URLSearchParams(window.location.search).get("api_key");
const snapshotUrl = apiKey ? `/live/snapshot?api_key=${encodeURIComponent(apiKey)}` : "/live/snapshot";
let polling = false;

async function poll() {
  try {
    const response = await fetch(snapshotUrl);
    if (response.ok) {
      connection.textContent = "Live (polling)";
      renderFrame(await response.json());
    } else {
      connection.textContent = `Disconnected (${response.status})`;
    }
  } catch {
    connection.textContent = "Disconnected";
  }
  setTimeout(poll, 1000);
}

function startPolling() {
  if (polling) return;
  polling = true;
  poll();
}

function connect() {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${protocol}://${window.location.host}/ws/live`);
  let opened = false;

  socket.onopen = () => {
    opened = true;
    connection.textContent = "Live";
  };

  socket.onmessage = (event) => {
    renderFrame(JSON.parse(event.data));
  };

  socket.onclose = () => {
    connection.textContent = "Disconnected";
    if (opened) {
      setTimeout(connect, 1000);
    } else {
      // WebSockets unavailable (e.g. serverless hosting); fall back to REST polling.
      startPolling();
    }
  };
}

const researchUrl = apiKey ? `/research/digest?api_key=${encodeURIComponent(apiKey)}` : "/research/digest";

async function pollResearch() {
  try {
    const response = await fetch(researchUrl);
    if (response.ok) {
      const data = await response.json();
      const bySymbol = {};
      for (const insight of (data.insights || [])) {
        for (const sym of (insight.symbols || [])) {
          if (!bySymbol[sym]) bySymbol[sym] = [];
          bySymbol[sym].push(insight);
        }
      }
      researchBySymbol = bySymbol;
    }
  } catch {
    // non-fatal: research panel stays blank until next poll
  }
  setTimeout(pollResearch, 60000);
}

loadObsidianProject();
pollResearch();
connect();

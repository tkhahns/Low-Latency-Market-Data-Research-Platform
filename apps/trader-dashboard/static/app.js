const watchlist = document.querySelector("#watchlist");
const connection = document.querySelector("#connection");
const symbolCount = document.querySelector("#symbol-count");
const obsidianLink = document.querySelector("#obsidian-link");

function money(value) {
  if (value === undefined || value === null) return "-";
  return Number(value).toFixed(2);
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

loadObsidianProject();
connect();

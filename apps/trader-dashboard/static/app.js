const watchlist = document.querySelector("#watchlist");
const connection = document.querySelector("#connection");
const symbolCount = document.querySelector("#symbol-count");

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

function connect() {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${protocol}://${window.location.host}/ws/live`);

  socket.onopen = () => {
    connection.textContent = "Live";
  };

  socket.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    symbolCount.textContent = payload.symbols.length;
    watchlist.innerHTML = payload.symbols.map(render).join("");
  };

  socket.onclose = () => {
    connection.textContent = "Disconnected";
    setTimeout(connect, 1000);
  };
}

connect();

# Vercel Deployment (Dashboard + API) and Custom Domain

Vercel runs **stateless serverless functions** — it cannot host the streaming backend
(Redpanda/Kafka, the feed ingestor, the stream processor, or Redis). What it *can* host
very well is the user-facing layer: the FastAPI Market Data API and the trader dashboard.

```
┌────────────────────────── Vercel ──────────────────────────┐
│  trader dashboard (static)  +  Market Data API (FastAPI)   │
└──────────────────────────────┬─────────────────────────────┘
                               │ reads
                  ┌────────────▼────────────┐
                  │  Managed Redis (Upstash)│   ← only needed in live mode
                  └────────────▲────────────┘
                               │ writes
        Pipeline runs elsewhere: GKE (deploy.yml), a VM, or
        any Docker host running the coinbase profile
```

Two supported modes:

| Mode | Env vars on Vercel | Backend required |
| --- | --- | --- |
| **Demo** (default recommendation) | `MARKET_DATA_DEMO_MODE=1` | None — self-contained seeded data |
| **Live** | `REDIS_URL=rediss://...` (e.g. Upstash) | Pipeline running elsewhere, writing to that Redis |

One behavioral note: Vercel functions do not support WebSocket servers, so the dashboard
automatically falls back from `/ws/live` to polling the REST endpoint `/live/snapshot`
once per second. The connection badge shows **“Live (polling)”** in this mode.

## Files that make this work

- `main.py` — root entrypoint exporting the FastAPI `app` (Vercel's FastAPI
  preset only looks in default locations like a root `main.py`)
- `requirements.txt` — deployment-only dependencies (`fastapi`, `redis`, `uvicorn`)
- `vercel.json` — pins the framework to `fastapi`
- `.vercelignore` — keeps infra/tests/docs out of the bundle

## Deploy — Option A: Git import (recommended)

1. Go to <https://vercel.com/new> and import `tkhahns/Low-Latency-Market-Data-Research-Platform`.
2. Framework preset: **FastAPI** (auto-detected; `vercel.json` pins it). Leave build
   command and output directory empty.
3. Under **Environment Variables**, add:
   - `MARKET_DATA_DEMO_MODE` = `1` (demo mode), **or**
   - `REDIS_URL` = your Upstash connection string (live mode)
   - optional: `API_KEYS` = comma-separated keys to require `X-API-Key` /
     `?api_key=` on data endpoints (the dashboard forwards `?api_key=` from its own URL)
   - optional: `CORS_ORIGINS` = comma-separated allowed origins (defaults to `*`)
4. Click **Deploy**. Every push to `main` redeploys production; every PR gets a preview URL.

Verify after deploy:

```bash
curl https://<project>.vercel.app/health
curl https://<project>.vercel.app/live/snapshot
open https://<project>.vercel.app/        # dashboard, badge shows "Live (polling)"
```

## Deploy — Option B: CLI

```bash
npm i -g vercel
vercel login
vercel link                      # create/link the project
vercel env add MARKET_DATA_DEMO_MODE production   # enter: 1
vercel --prod
```

## Live mode with Upstash Redis (free tier)

1. Create a database at <https://console.upstash.com> (free tier is fine) and copy the
   `rediss://default:<password>@<host>:6379` URL.
2. Set it as `REDIS_URL` on both sides:
   - **Vercel** project env var (the API reads from it), and
   - the **pipeline host** (the stack writes to it). For a single Docker host:

     ```bash
     REDIS_URL='rediss://default:<password>@<host>:6379' \
       docker compose -f infra/docker-compose.yml --profile coinbase up --build \
       redpanda feed-handler stream-processor coinbase-feed
     ```

     (note: no local `redis` or `market-data-api` services — Upstash and Vercel replace them).
   - For GKE, set `REDIS_URL` in `infra/kubernetes/base/configmap.yaml` via overlay.
3. Redeploy on Vercel; `/symbols` should now show `BTC-USD`, `ETH-USD`, `SOL-USD`.

## Custom domain

### 1. Add the domain to the project

Vercel dashboard → your project → **Settings → Domains** → enter your domain
(e.g. `marketdata.example.com` or apex `example.com`) → **Add**. Or via CLI:

```bash
vercel domains add example.com
```

If you don't own a domain yet, you can buy one directly in that screen
(**Domains → Buy**) and skip the DNS section entirely — Vercel configures everything.

### 2. Point DNS at Vercel

Pick one of these at your registrar (Namecheap, GoDaddy, Cloudflare, etc.):

| Record type | Host | Value | Use for |
| --- | --- | --- | --- |
| `A` | `@` | `76.76.21.21` | Apex domain (`example.com`) |
| `CNAME` | `www` (or any subdomain) | `cname.vercel-dns.com` | Subdomains |

Or hand the whole zone to Vercel by switching nameservers to
`ns1.vercel-dns.com` and `ns2.vercel-dns.com` (then Vercel manages all records,
which also enables apex + wildcard with zero extra setup).

Notes:

- If the domain is already used by another Vercel account, Vercel shows a `TXT`
  record (`_vercel` host) to prove ownership — add it and click **Verify**.
- If you use Cloudflare, set the record to **DNS only** (grey cloud) so Vercel
  can issue certificates.

### 3. TLS and redirects (automatic)

Once DNS propagates (usually minutes, up to 48 h), Vercel issues a Let's Encrypt
certificate automatically and renews it forever. In **Settings → Domains** set your
preferred domain (e.g. `www` → apex redirect, 308) — one click, no config files.

Verify:

```bash
curl -I https://example.com/health     # expect 200 and a valid cert
```

## Limits to be aware of

- **No WebSockets** — handled by the dashboard's polling fallback.
- **Function duration** — 10 s default on Hobby; all endpoints here respond in
  milliseconds, so this is not a constraint.
- **In-process rate limiting** — token buckets are per serverless instance, so the
  effective rate limit scales with concurrency. For strict global limits, enforce at
  an edge middleware or upstream gateway.
- **Cold starts** — first request after idle takes ~1–2 s; subsequent requests are fast.

The full Kubernetes/GCP path for the entire pipeline remains documented in
`docs/production-readiness.md` and automated in `.github/workflows/deploy.yml`.

# Deployment guide

This repository is deployed as one persistent Node control-plane process around the existing Python research engine. It is an internal research and paper-simulation service: it never connects to a broker or submits an external order.

## Production topology

```text
BaoTa Nginx (HTTPS / local access control)
  -> 127.0.0.1:3210 Node service, exactly one instance
       -> static React console + read-only status/log APIs
       -> fixed Asia/Shanghai schedules
       -> allow-listed Python commands in .venv
            -> existing A1-A4 workflow / exchange calendar / SQLite leases
            -> state, storage, outputs and cache
```

Node owns the production schedule. Do **not** also enable the legacy systemd timers, cron file or Windows scheduled tasks: doing so would create duplicate dispatch attempts. Python's exchange calendar and SQLite leases remain the final holiday and idempotency authority.

## BaoTa deployment

Requirements:

- Linux x86_64 virtual machine
- Node 20 LTS or 22 LTS
- Python 3.11 or newer
- one non-root application user
- BaoTa Node Project Manager and Nginx

Assuming the repository is at `/www/wwwroot/liangjian-funnel-workflow`:

```bash
cd /www/wwwroot/liangjian-funnel-workflow
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install ".[dev]"
npm ci
npm run build
mkdir -p outputs/node outputs/scheduler state storage cache
```

Copy `.env.example` to `.env`, mode `0600`, and fill secrets only on the server. Existing Python settings load this file automatically; process environment values take precedence. Set the Node-specific variables in BaoTa's project environment:

```text
NODE_ENV=production
HOST=127.0.0.1
PORT=3210
TZ=Asia/Shanghai
LIANGJIAN_PYTHON_BIN=.venv/bin/python
LIANGJIAN_WEB_DIST=dist/web
LIANGJIAN_SCHEDULER_ENABLED=true
LIANGJIAN_DASHBOARD_TOKEN=<long random value>
```

BaoTa project parameters:

- project directory: repository root
- install command: `npm ci`
- build command: `npm run build`
- startup file: `dist/server/index.js`
- instances: `1`; never use PM2 cluster mode for this scheduler
- restart policy: automatic, with at least 15 seconds graceful-stop allowance

Use [deploy/baota/nginx.conf.example](deploy/baota/nginx.conf.example) for the reverse proxy. The service binds localhost by default. For private use, add a BaoTa IP allow-list, HTTP Basic Auth or VPN in addition to the optional dashboard bearer token. The token is held only in browser session storage.

## Fixed schedules

All schedules use `Asia/Shanghai`:

- 09:26 on weekdays: `python -m liangjian_funnel run-morning`
- 15:10 on weekdays: `python -m liangjian_funnel run-close` (active A1 → A2 → A3)
- 18:00 on weekdays: `python -m liangjian_funnel run-a1-maintenance`; Python publishes a monthly FULL on the first exchange session, a weekly INCREMENTAL on the last exchange session, and returns NOOP otherwise. A fresh deployment with no active generation performs one bootstrap FULL.
- each minute in 09:25-11:30 and 13:00-15:00 on weekdays: `python -m liangjian_funnel run-monitor`

For production stable mode, set `LIANGJIAN_COMPARISON_ENABLED=false`. The
scheduled active-A1 → A2→A3 primary path remains DeepSeek-only and serial; optional
Kimi/GLM comparisons are not recovered on Node startup and are not enqueued
after a successful close run. This flag does not permit real orders and does
not relax any A3/A4 gate.

The Node runner uses a strict allow-list, single-process overlap protection, bounded child-process handling and graceful shutdown. The Python commands still enforce the actual exchange trading calendar, dispatch key and SQLite lease. `run-monitor` legitimately returns empty scope when no active A3 plan exists.

For UI/API diagnostics that must not dispatch scheduled work, start a temporary instance with `LIANGJIAN_SCHEDULER_ENABLED=false`. Do not use that value for unattended production.

## Deployment gate

Run this before letting BaoTa keep the process online:

```bash
npm ci
npm run typecheck
npm test
npm run build
.venv/bin/python -m pytest
.venv/bin/python -m compileall -q src
.venv/bin/python -m liangjian_funnel doctor
.venv/bin/python -m liangjian_funnel probe-all
.venv/bin/python -m liangjian_funnel status
```

Then start the Node project and verify through the reverse proxy:

```bash
curl -fsS http://127.0.0.1:3210/api/health
curl -fsS -H "Authorization: Bearer $LIANGJIAN_DASHBOARD_TOKEN" http://127.0.0.1:3210/api/overview
```

`status.configuration_ready` checks local persistence, credentials and exchange rules. `status.deployment_ready` additionally requires the newest persisted full run to have all three lanes ready or published. A minimal model probe is not proof that a full A1 prompt is operational; perform one bounded close-sized validation before unattended use.

### Manual replay and acceptance process identity

BaoTa runs this application as its configured non-root application user (the current VM uses `www:www`). Any manual replay, feature rebuild or transient systemd acceptance unit **must run as that same user and group**. The workflow intentionally writes atomic result files with mode `0600`; running a replay as `root` would therefore create valid results that the Node dashboard cannot read.

For the current BaoTa layout, use a transient service with the following process boundary (and keep the existing simulation-only environment):

```ini
[Service]
Type=oneshot
User=www
Group=www
WorkingDirectory=/www/wwwroot/Agu/liangjian-funnel-workflow
EnvironmentFile=/www/wwwroot/Agu/liangjian-funnel-workflow/.env
ExecStart=/www/wwwroot/Agu/liangjian-funnel-workflow/.venv/bin/python -m liangjian_funnel run-close
```

Do not solve a user mismatch by weakening result permissions to world-readable. If an earlier administrative replay created root-owned runtime files, stop that replay, verify the resolved project path, and restore ownership only under the application's `outputs/`, `state/` and `storage/` runtime subtrees. Never recursively change ownership of `.env`, `.git` or the whole server root.

## Logs and persistence

- `outputs/node/node-YYYY-MM-DD.jsonl`: Node scheduler and child-process logs, structured and redacted.
- `outputs/scheduler/*.log`: existing Python scheduler output.
- `outputs/research/*.md` and `*_lane_*.json`: research reports and lane audits.
- `outputs/monitor/effective_signals.md`: effective intraday events only.
- `state/workflow.sqlite3`: plans, events, simulated accounts, positions, fills and leases.
- `storage/`: immutable snapshots, fact evidence and caches.

The dashboard never returns `.env`, authorization headers or model reasoning text. Keep Nginx access logs and BaoTa process logs under normal rotation as a separate infrastructure layer.

Back up `state/workflow.sqlite3`, `state/a1_registry.sqlite3`, `storage/` and `outputs/` together. Keep `.env` in a separately encrypted backup. The application runtime directories must survive code releases and container replacement.

## Upgrade and rollback

Before an upgrade, stop the BaoTa Node project and take a consistent backup. Then:

```bash
git pull --ff-only
npm ci
npm run build
.venv/bin/python -m pip install ".[dev]"
npm run typecheck
npm test
.venv/bin/python -m pytest
```

Start the project only after validation passes. Roll back by stopping Node, restoring the prior code revision and its matching state/storage backup, rebuilding, and starting one instance. Do not reuse a newer SQLite file with an older binary without restoring the matching backup.

## Container image

`docker build -t liangjian-funnel-console:1.0.0 .` builds Node and Python into one image and starts the dashboard on port 3210. Mount `state`, `storage`, `outputs` and `cache`, inject secrets at runtime, and run one container instance. Host cron must remain disabled when the Node scheduler is active.

## Legacy scheduler assets

`deploy/systemd/` and `deploy/cron/` are retained for deployments that run the Python workflow without the Node control plane. They are mutually exclusive with the BaoTa Node project and should not be installed on the same active deployment.

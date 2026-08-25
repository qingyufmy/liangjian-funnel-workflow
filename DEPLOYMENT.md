# Deployment guide

This is an internal research and paper-simulation service. It never connects
to a broker or submits an external order.

## Linux host installation

Use a dedicated, non-login service account and an absolute application path:

```bash
sudo useradd --system --home /opt/liangjian-funnel-workflow --shell /usr/sbin/nologin liangjian
sudo -u liangjian python3.11 -m venv /opt/liangjian-funnel-workflow/.venv
sudo -u liangjian /opt/liangjian-funnel-workflow/.venv/bin/pip install "/opt/liangjian-funnel-workflow[dev]"
sudo -u liangjian mkdir -p /opt/liangjian-funnel-workflow/outputs/scheduler /opt/liangjian-funnel-workflow/state /opt/liangjian-funnel-workflow/storage /opt/liangjian-funnel-workflow/cache
sudo -u liangjian /opt/liangjian-funnel-workflow/.venv/bin/liangjian-funnel doctor
sudo -u liangjian /opt/liangjian-funnel-workflow/.venv/bin/liangjian-funnel probe-all
```

Put secrets only in `/opt/liangjian-funnel-workflow/.env`, mode `0600`. Start
with `HITHINK_FINANCE_API_KEY` and `LIANGJIAN_MODEL_API_KEY`; never bake them
into an image or unit file. The application writes only to `outputs/`,
`state/`, `storage/` and `cache/`. Back up `state/workflow.sqlite3` together
with the immutable snapshots.

For the full research payload, the deployment defaults are
`LIANGJIAN_MODEL_TIMEOUT_SECONDS=300`,
`LIANGJIAN_MODEL_MAX_OUTPUT_TOKENS=6000` and
`LIANGJIAN_A1_BATCH_SIZE=5`. A low-frequency host may raise the timeout to 600
seconds, but must keep the prompt/output limits. The close systemd unit allows
five hours so four sequential A1 batches per lane can finish while the three
lanes remain parallel.

Copy the two research services/timers from `deploy/systemd/` to
`/etc/systemd/system/`, review the user and paths, then enable them. Install
the explicit minute ranges from `deploy/cron/liangjian.crontab.example` for
monitoring. The internal XSHG calendar is authoritative for SH/SZ holiday
skips; weekday scheduling alone is not treated as a trading calendar.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now liangjian-morning.timer liangjian-close.timer
sudo -u liangjian crontab /opt/liangjian-funnel-workflow/deploy/cron/liangjian.crontab.example
```

## Deployment gate

Before enabling schedules, all of these must pass:

```bash
.venv/bin/python -m pytest
.venv/bin/python -m compileall -q src
.venv/bin/liangjian-funnel doctor
.venv/bin/liangjian-funnel probe-all
.venv/bin/liangjian-funnel status
```

`status.configuration_ready` checks local persistence, both credentials and the
exchange-rule snapshot. `status.deployment_ready` additionally requires the
newest persisted full run to have all three lanes ready or published. A
successful minimal model probe is not proof that a full A1 prompt is
operational: perform one close-sized dry run and confirm all lane/stage states
in `latest_workflow_runs` before unattended use.

An immutable close snapshot can be replayed without fetching market data
again. A validated upstream audit can also resume only A2 or A3:

```bash
.venv/bin/python scripts/replay_frozen_research.py --snapshot storage/snapshots/snapshot-....json --slot close
.venv/bin/python scripts/replay_frozen_research.py --snapshot storage/snapshots/snapshot-....json --resume-audit outputs/research/research_..._lane_2.json --stage A3
```

Both paths restrict files to configured output directories and verify snapshot
lineage/hash before making a model request.

The following business inputs remain explicit external prerequisites rather
than fabricated fallbacks: industry profit data, a versioned industry-chain
revenue graph, sector history/capital flow, fund holdings/crowding, theme
registry and research consensus. Missing evidence remains fail-closed and can
legitimately produce no plans.

## Container image

`docker build -t liangjian-funnel:0.1.0 .` creates a runnable image. Mount the
four writable directories and inject `.env` at runtime. Run the dedicated
commands (`run-morning`, `run-close`, `run-monitor`) from the host scheduler;
do not run multiple scheduler loops in one container.

Rollback by stopping timers/cron and switching the application directory or
image tag back to the previously validated release. Do not reuse a newer
SQLite file with an older binary without first restoring its matching backup.

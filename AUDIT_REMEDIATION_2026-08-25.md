# 2026-08-25 audit remediation and live acceptance

This document supersedes status claims made before the 2026-08-25 remediation
branch. It distinguishes implemented controls from live end-to-end evidence.

## Live acceptance result

- Latest full data snapshot: `snapshot-20260825T163244+0800-4d48ab813482`.
- Full universe: 5,561; research universe: 2,834; trade universe: 2,806.
- Requested/factor-ready candidates: 20/20. The complete snapshot hash was
  verified before every replay.
- Minimal gateway probes passed for all configured models.
- Full A1 originally timed out because the rendered request was approximately
  790k characters and asked for an unbounded full report.
- The production view now retains the full immutable snapshot for audit while
  sending bounded facts, five-symbol A1 batches, a compact downstream contract,
  `reasoning_effort=low`, a 6,000-token output ceiling and a 300-second request
  deadline (configurable up to 600 seconds).
- `moonshotai/kimi-k3-free` completed four A1 batches, validated A2, and
  completed a validated A3 replay. A3 deterministic prices were canonicalized
  from frozen `PRICE_LEVELS` rather than trusting rounded model numbers.
- The only final core item, `300750.SZ`, was changed by the deterministic trend
  governor to `NO_ENTRY`: both daily and 120-minute closes were below MA255.
  The result remains an observation plan and cannot be published as a simulated
  buy.
- DeepSeek and GLM long-form lanes remain provider-unstable (network deadline or
  invalid strict JSON). Lane isolation remains fail-closed; they cannot create
  plans or contaminate a validated lane.

The A1 -> A2 -> A3 AI chain is therefore proven for one real lane and the same
real snapshot. Three-lane reliability and a real A4/buy/exit cycle are not yet
accepted.

## Audit issue disposition

| Audit item | Disposition |
|---|---|
| P0-1 full-model failure | Partially accepted live: Kimi A1-A3 validated; bounded prompts, batching, safe SSE diagnostics, total deadlines, compact contract, replay/resume tooling added. DeepSeek/GLM remain external reliability gaps. |
| P0-2 15:10 task race / false success | Fixed: dedicated morning/close/monitor commands and nonzero propagation for business `BLOCKED`; monitor ends at 15:00. |
| P0-3 weekday-only calendar | Fixed: XSHG exchange calendar is injected and fails closed; holiday tests included. |
| P0-4 morning misses 09:40 | Fixed structurally: 09:26 performs deterministic tighten-only review of prior close plans, not a new full A1-A3 run. |
| P0-5 SELL/REDUCE ignored | Fixed and covered: SELL, REDUCE and forced exits settle on the next closed bar. |
| P0-6 risk plans/governor/actions | Fixed: independent deterministic governor, durable position risk plans, profit-only bounded adds, real-trading-date T+1, marks and idempotent verified corporate actions. |
| P1-1 missing business facts | External prerequisite, not fabricated. Industry profit, versioned chain graph, sector capital history, full holdings/crowding, theme registry and consensus remain explicit missing sources. |
| P1-2 candidate degradation | Fixed: ordered reserve candidates backfill failed history/fundamental candidates until 20 factor-ready names or exhaustion. |
| P1-3 no real A4 plan lifecycle | Still open as live acceptance. The current real A3 item is correctly `NO_ENTRY`, so no artificial trade was created. |
| P1-4 interactive Windows only | Deployment alternative added: Linux systemd/cron and Docker assets support a dedicated non-login service account. |
| P1-5 incomplete A4 contract | Fixed in code: shared per-minute 1m/5m fetch, closed 15m derivation, moving averages/VWAP, deterministic tradability and sector context when available. Missing sector facts remain fail-closed. |
| P1-6 Flash no retry | Fixed: bounded local retry including `Retry-After`; no cross-minute circuit breaker. |
| P1-7 crash lease cannot recover | Fixed and tested: expired same-dispatch leases can be reacquired; completion state prevents duplicate completed work. |
| P1-8 A4 lanes serial | Fixed: non-empty lanes run in parallel against one frozen minute snapshot. |
| P2-1 weak status | Fixed: latest workflow/lane/stage, reasons, source failures, probes and deployment readiness are exposed. |
| P2-2 weak model diagnostics | Fixed: safe event/output shapes, attempts and real elapsed time are retained without content, secrets or reasoning. |
| P2-3 documentation drift | Current status and this disposition are the authoritative entry points; older acceptance documents remain historical snapshots. |
| P2-4 backup/retention | Deployment guide requires matched SQLite/snapshot backups. Automated deletion/rotation is intentionally not included because retention policy and backup target are deployment-specific. |
| P2-5 portfolio mark-to-market | Fixed: every position uses its own persisted latest mark for equity and total-exposure checks. |
| P2-6 T+1 day roll | Fixed: idempotent real-trading-day initialization runs from independent daily/monitor entry paths. |
| P2-7 append-only Markdown gap | Fixed: SQLite is authoritative and effective Markdown is atomically rebuilt. |
| P2-8 effective-event meaning | Documented as state/risk-changing events, including veto, invalidation and data block; `NO_ACTION` is excluded. |

## Remaining deployment gates

Do not label the service fully unattended/three-lane accepted until:

1. DeepSeek and GLM each complete repeated close-sized runs, or the approved
   production model set is explicitly changed.
2. The external business fact sources listed above are configured and their
   point-in-time contracts pass.
3. A naturally generated executable plan completes close publication, morning
   tightening, A4 monitoring, simulated entry, T+1 and normal reduce/exit.
4. The target server's backup destination, retention period and alert channel
   are configured and tested.

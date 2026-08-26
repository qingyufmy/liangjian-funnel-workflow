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
  `reasoning_effort=low`, a 6,000-token output ceiling and a 600-second request
  deadline. Failed multi-symbol A1 transport groups can split without repeating
  already validated groups.
- The v10 replay completed `READY` for DeepSeek, Kimi and GLM; every A1-A3 stage
  was `VALIDATED`. A1 classified all 20 inputs exactly once. The respective
  ACTIVE/MONITOR/REJECT counts were 0/20/0, 7/13/0 and 3/16/1.
- Batch size no longer acts as a global pool cap. Production covers up to 1,000
  macro/fundamental-ready candidates, and technical readiness is enforced in A3; the 20-name replay
  is a chain-acceptance sample, not the production A1 breadth.
- YAML thresholds are enforced by the service after model interpretation: A1
  score/data-quality/evidence thresholds, A2 minimum theme score, and A3
  technical-score/reward-risk/stop-distance gates. Provider JSON can no longer
  promote a below-threshold item by assertion alone.
- A3 deterministic prices were canonicalized from frozen `PRICE_LEVELS` rather
  than trusting rounded model numbers. Kimi's only core observation,
  `002837.SZ`, was forced to `NO_ENTRY` by the deterministic major-trend gate;
  GLM produced no core plan and DeepSeek correctly produced `NO_ACTION`.

The A1 -> A2 -> A3 AI chain is therefore accepted for all three configured
models on the same real snapshot. A naturally generated executable plan and
real A4/buy/exit cycle are still not accepted.

## Audit issue disposition

| Audit item | Disposition |
|---|---|
| P0-1 full-model failure | Accepted live for A1-A3: all three configured model lanes completed `READY` on v10. Bounded prompts, 600-second deadlines, safe SSE/JSON compatibility, semantic retry, failed-batch splitting, deterministic no-action stages and replay/resume tooling are included. |
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

1. The external business fact sources listed above are configured and their
   point-in-time contracts pass.
2. A naturally generated executable plan completes close publication, morning
   tightening, A4 monitoring, simulated entry, T+1 and normal reduce/exit.
3. The target server's backup destination, retention period and alert channel
   are configured and tested.

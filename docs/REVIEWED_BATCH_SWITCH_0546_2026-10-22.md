# 0546 reviewed batch foundation — 2026-10-22

Status: local implementation only; not deployed or accepted as a real-provider whole chain.
The later ruling 0547 cancels pre-open deployment/replacement. Existing three plans remain untouched.

## Current implementation

1. `reviewed_batch_evidence.build_reviewed_batch_evidence(...)` reads isolated `team.db` and `model.db` with `mode=ro`, `query_only`, bounded original records. It verifies the actual engine, distinct scoped A6/PM meeting and call IDs, capture/claim/judgment/catalog hashes, source calendar, A6 result recomputation and PM source-link projection. It does not accept a caller-supplied green flag or perform model calls.
2. `RuntimeStore.stage_reviewed_batch(bundle)` stores an immutable READY bundle without publishing plans or changing the active pointer.
3. `RuntimeStore.commit_reviewed_batch_switch(scope_id, expected_old_rows_sha256=..., expected_old_plan_ids=..., expected_pointer_revision=...)` performs new-plan insertion, old-batch retirement, pointer CAS and audit in one workflow-database transaction. The old batch can contain PENDING and ACTIVE rows. A concurrent write, failed review, malformed proof, interrupted transaction or second racing switch leaves the previous batch intact.
4. At least one actual PM primary allocation must intersect an eligible A6 envelope with positive existing risk budget, shares and batches. A legal zero-allocation research result is not permission to replace the old batch.
5. Each Team monitor tick reads one immutable reviewed scope. Its execution-plan transaction checks the same pointer revision; consumers use the bundle's original A6 and PM decisions. With no pointer, the legacy morning path remains in effect. Existing holding owners, paper positions, dated lots, T+1 tasks and record latch remain in their original stores.
6. `reviewed_batch_morning.prepare_candidate_morning_engine(...)` reads the actual natural-morning account/held-owner/external evidence, verifies their original hashes and recomputes the candidate's planning envelopes without a broker, collection or account reinitialization. `run_candidate_morning_reviews(...)` uses independent scoped IDs and existing A6/PM business sessions before 09:26; failed/unvalidated A6 prevents PM, and PM fallback is disabled. All new meeting/model records stay under the isolated root.
7. `python -m liangjian_funnel.runtime.reviewed_batch_cli` exposes `review-isolated`, `stage-reviewed`, `switch-reviewed` and RO `inspect`. Explicit model/write flags are required before configuration/Store access. Staging always rebuilds the bundle from actual isolated review originals; it cannot import a caller-authored READY JSON. The switch consumes a bounded CAS file with exactly the old-row SHA, full old-plan IDs and expected pointer revision.

## Explicit limits

- Commit is **PRE_AUCTION only**, using the real target-day clock before 09:26. New rows remain PENDING with null `valid_from`; A6/PM qualification does not invent auction activation. The existing auction validation/activation API is still required.
- This prevents the natural 09:30+ A4 order-processing path from overlapping a switch. Per-tick pointer checks are not a cross-database transaction or a general intraday switch fence. Do not relax this window without a separately reviewed design and tests.
- The separate candidate review and import/switch entry is implemented. The automatic natural-close staging policy is not rewired here: the existing production close path is unchanged. Its later integration must not be inferred from the manual API or claimed complete.
- A6 raw `validated=0`, even if quota normalization later creates effective READY, is deliberately rejected by the evidence importer. PM fallback is never accepted as model-reviewed qualification.
- Do not edit original plans, captures, claims or supersession records with SQL. Do not reset a failed research marker. Rollback of a deployed version follows the existing forward rollback + record procedure; no automatic restoration of superseded historical rows is claimed here.

## Verification and release

Controlled local tests cover strict evidence, mixed old statuses, full-row/source/pointer CAS, mid-transaction rollback, two writers, corrupted bundles, zero PM/A6 capacity, unchanged 002270 owner and separate auction activation followed by new-only A4 scope. They use explicit test clocks and Mock provider responses, not real business or production evidence.

VM own-full, the same final HEAD's deterministic 40, rollback confirmation, real-provider review evidence and Claude final ACCEPT remain mandatory before release. The separate 0547 isolated research uses exact source HEAD 026195b and frozen source 10-21 → target 10-22; it must not be relabeled as a real-model run of this later candidate HEAD.

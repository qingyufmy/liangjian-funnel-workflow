# Research outcome status contract

`research-outcome/3.0.0` is the only canonical result exchanged between the
Python workflow, CLI, Node control plane and web UI.  The contract is a set of
independent axes; a legacy `status`/`opportunity_state` is a read-only
projection for old artifacts and must not drive a new business decision.

## Axes

| Axis | Values | Meaning |
| --- | --- | --- |
| `job_status` | `QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`, `STALE` | Process lifecycle. A data gate that was evaluated is `SUCCEEDED`. |
| `lifecycle_state` | `QUEUED`, `RUNNING`, `TERMINAL` | Backward-compatible coarse lifecycle view. |
| `quality_state` | `VALIDATED`, `DEGRADED`, `BLOCKED`, `FAILED`, `CANCELLED` | Evidence/execution quality, independent of opportunity. |
| `data_sufficiency_state` | `SUFFICIENT`, `PARTIAL`, `INSUFFICIENT`, `NOT_APPLICABLE` | Whether the relevant evidence domain supports a conclusion. |
| `research_opportunity_state` | `PRESENT`, `ABSENT`, `UNKNOWN`, `NOT_APPLICABLE` | A1 macro/fundamental research result only. |
| `focus_opportunity_state` | `PRESENT`, `ABSENT`, `UNKNOWN`, `NOT_APPLICABLE` | A2 theme/industry/flow focus result only. |
| `actionability_state` | `ACTIONABLE`, `NO_ACTION`, `UNKNOWN`, `NOT_APPLICABLE` | A3 technical entry/exit plan only. |
| `publication_state` | `READY`, `PUBLISHED`, `BLOCKED`, `NOT_APPLICABLE` | Whether the available result may be published. |

The three business axes are stage-scoped.  A1 `PRESENT` must never be copied
to A2 or A3.  A3 `ACTIONABLE` can only come from an executed A3 plan; a
downstream stage that was not eligible is `NOT_APPLICABLE`, not `NO_ACTION`.

## Required mappings

### A2 evidence gap

When the A2 factor/flow/ladder evidence is insufficient, but the process
completed its deterministic/data gate:

```text
A2: quality=DEGRADED, data_sufficiency=INSUFFICIENT,
    focus_opportunity=UNKNOWN, actionability=NOT_APPLICABLE
A3: job_status=SUCCEEDED, quality=DEGRADED,
    actionability=NOT_APPLICABLE, publication=NOT_APPLICABLE,
    reason_codes includes A3_NOT_APPLICABLE_UPSTREAM_DATA_GAP
lane/run: job_status=SUCCEEDED, quality=DEGRADED,
          focus_opportunity=UNKNOWN, actionability=NOT_APPLICABLE,
          publication=READY, legacy_status=READY_DEGRADED
```

This is a publishable degraded research result, not an execution failure and
not a false A3 trade signal.  A technical/model error remains
`quality=FAILED` and `publication=BLOCKED`.

### Validated empty result

An empty A2/A3 result is `ABSENT`/`NO_ACTION` only when the input and
evaluated denominators prove complete coverage (`evaluated >= input`, or an
equivalent explicit coverage pair).  A zero count without that denominator
is `UNKNOWN` plus `data_sufficiency=INSUFFICIENT`.

### Primary and optional lanes

Only primary lanes determine run quality/publication.  Optional comparison
lanes may be `FAILED` without blocking a ready primary result; their state is
reported through `comparison_status`.  A missing primary lane is a hard
`BLOCKED` result.

## Progress contract

`workflow-progress/3.0.0` stores a root `job_status`, per-lane/per-stage
`job_status`, and optional bounded `outcome_v3`.  `finish()` changes root
terminal state only; it never overwrites existing lane or stage states.  The
canonical outcome is authoritative for `job_status` when supplied.  Progress
is a presentation snapshot and is not an execution lease.

## Legacy compatibility

Readers accept v2/legacy artifacts at one boundary and project them to v3.
Writers emit v3 and, where needed for old read-only consumers, a nested
`legacy_projection` with `schema_version=research-outcome/2.0.0`.  Do not
persist a second independently computed v2 result.  CLI construction may use
`stage_outcome_from_legacy(status, stage=..., counts=..., data_coverage=...)`
and then `aggregate_lane_outcome`/`aggregate_run_outcome`; new code should
construct `StageOutcome` with the v3 axes directly and call `as_dict()`.

## Contract checks

Run the dependency-free check from the repository root:

```powershell
python scripts/export_outcome_contract.py --check
```

The same check is part of CI and verifies the JSON Schema, Python reducer and
Node/web projections carry the same version and vocabulary.

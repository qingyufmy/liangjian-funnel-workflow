/** Safe, read-only display semantics persisted by the Python research path. */
export const RESEARCH_PRESENTATION_SCHEMA = "research-presentation/1.0.0";

type RecordValue = Record<string, unknown>;
const record = (value: unknown): RecordValue => value && typeof value === "object" && !Array.isArray(value) ? value as RecordValue : {};
const array = (value: unknown): unknown[] => Array.isArray(value) ? value : [];
const strings = (value: unknown): string[] => [...new Set(array(value).filter((item): item is string => typeof item === "string" && item.trim().length > 0).map((item) => item.trim()))];
const first = (value: RecordValue, keys: readonly string[]): unknown => {
  for (const key of keys) if (value[key] !== undefined && value[key] !== null && value[key] !== "") return value[key];
  return null;
};
const number = (value: unknown): number | null => typeof value === "number" && Number.isFinite(value) ? value : null;
const text = (value: unknown): string | null => typeof value === "string" && value.trim() ? value.trim() : null;
const safeTime = (value: unknown): number | null => {
  const parsed = typeof value === "string" ? Date.parse(value) : Number.NaN;
  return Number.isFinite(parsed) ? parsed : null;
};

export interface A1Presentation {
  admissionPath: string | null;
  researchLevel: string;
  permissionMeaning: string;
  criticalGaps: string[];
  knownNegativeFacts: string[];
  evidenceAsOf: string | null;
}

export interface A2Presentation {
  themeStrength: number | null;
  stockRelativeStrength: unknown;
  marketRole: unknown;
  researchPath: string | null;
  individualTotalScore: number | null;
}

export interface A3TargetPresentation {
  price: number | null;
  kind: string;
  basis: string | null;
  claim: string;
}

export interface A3Presentation {
  dailySetupState: string;
  a4ConfirmationState: string;
  currentEntryEligibility: string;
  planValidityState: string;
  validFrom: string | null;
  expiresAt: string | null;
  deferredConditions: string[];
  executionPermission: string | null;
  target: A3TargetPresentation;
}

export interface ResearchPresentation {
  schemaVersion: typeof RESEARCH_PRESENTATION_SCHEMA;
  stage: string;
  pool: string;
  displayScore: number | null;
  scoreMeaning: string;
  sourceIdentities: string[];
  legacyAdapter: boolean;
  a1?: A1Presentation;
  a2?: A2Presentation;
  a3?: A3Presentation;
}

function sourceIdentities(value: RecordValue): string[] {
  const output: string[] = [];
  for (const key of ["source_refs", "supporting_source_refs", "base_source_refs", "theme_source_refs", "node_source_refs", "evidence_refs"]) {
    for (const raw of array(value[key])) {
      if (typeof raw === "string" && raw.trim()) output.push(raw.trim());
      else {
        const item = record(raw);
        const identity = text(first(item, ["source_id", "source", "fact_id", "evidence_id", "content_hash"]));
        if (identity) output.push(identity);
      }
    }
  }
  return [...new Set(output)].slice(0, 100);
}

function explicitScore(value: RecordValue, stage: string): number | null {
  if (stage === "A1") return number(first(value, ["structural_score", "structuralScore", "composite_score", "total_score"]));
  if (stage === "A2") return number(first(value, ["stock_composite_score", "stock_total_score", "individual_total_score"]));
  return number(first(value, ["technical_score", "technicalScore"]));
}

function legacyTarget(value: RecordValue): A3TargetPresentation {
  const facts = record(value.strategy_facts);
  const targets = record(facts.observation_targets);
  const basis = text(first(value, ["target_basis", "pressure_basis"])) ?? text(targets.target_basis);
  const resistance = number(first(value, ["first_resistance", "pressure_reduce_price"]));
  const r2 = number(targets.r2);
  if (basis === "R_MULTIPLE_NO_RESISTANCE_REQUIRED" || (value.price_discovery === true && resistance === null && r2 !== null)) {
    return { price: r2, kind: "FIXED_R_OBSERVATION", basis: "R_MULTIPLE_NO_RESISTANCE_REQUIRED", claim: "OBSERVATION_NOT_MARKET_PROOF" };
  }
  if (resistance !== null) return { price: resistance, kind: "OBSERVED_RESISTANCE", basis: basis ?? "FIRST_RESISTANCE", claim: "MARKET_STRUCTURE_REFERENCE" };
  return { price: null, kind: "UNAVAILABLE", basis: null, claim: "NO_TARGET_EVIDENCE" };
}

export function legacyResearchPresentation(value: RecordValue, rawStage: string, rawPool: string, nowIso?: string): ResearchPresentation {
  const stage = rawStage.toUpperCase();
  const pool = rawPool.toLowerCase();
  const score = explicitScore(value, stage);
  const base: ResearchPresentation = {
    schemaVersion: RESEARCH_PRESENTATION_SCHEMA,
    stage,
    pool,
    displayScore: score,
    scoreMeaning: score === null ? "NO_INDIVIDUAL_TOTAL_SCORE" : "EXPLICIT_STAGE_SCORE",
    sourceIdentities: sourceIdentities(value),
    legacyAdapter: true,
  };
  if (stage === "A1") {
    const reasons = strings(value.reason_codes);
    const permission = pool === "approved" ? "RESEARCH_ADMITTED_NOT_QUALITY_CERTIFICATE" : pool === "watch" ? "RESEARCH_MONITOR_ONLY" : pool === "rejected" ? "RESEARCH_NOT_ADMITTED" : "RESEARCH_STATE_UNKNOWN";
    base.a1 = {
      admissionPath: text(first(value, ["admission_path", "selection_route", "candidate_origin", "route"])),
      researchLevel: text(first(value, ["research_level", "research_depth", "evidence_level"])) ?? "UNSPECIFIED",
      permissionMeaning: permission,
      criticalGaps: reasons.filter((code) => /MISSING|GAP|UNAVAILABLE|UNVERIFIED|INSUFFICIENT|NOT_PUBLISHED/.test(code.toUpperCase())),
      knownNegativeFacts: [...new Set([...strings(first(value, ["risk_reasons", "risk_flags", "known_negatives"])), ...reasons.filter((code) => /NEGATIVE|DECLINE|LOSS|WEAK|BEARISH|VETO|REJECT/.test(code.toUpperCase()))])],
      evidenceAsOf: text(first(value, ["evidence_as_of", "as_of", "observation_date", "publish_time"])),
    };
  } else if (stage === "A2") {
    const relative = first(value, ["relative_strength_score", "relativeStrengthScore", "relative_strength", "relativeStrength"]);
    base.a2 = {
      themeStrength: number(first(value, ["theme_score", "themeScore"])),
      stockRelativeStrength: number(relative) ?? (Object.keys(record(relative)).length ? record(relative) : null),
      marketRole: first(value, ["market_role", "marketRole", "leader_role", "leaderRole"]),
      researchPath: text(first(value, ["a2_route", "a2Route", "selection_route", "selectionRoute", "route"])),
      individualTotalScore: score,
    };
  } else if (stage === "A3") {
    const eligibility = text(first(value, ["deterministic_eligibility", "eligibility"]))?.toUpperCase() ?? "UNKNOWN";
    const permission = text(first(value, ["execution_permission", "route_permission"]))?.toUpperCase() ?? null;
    const deferred = strings(first(value, ["a4_deferred_conditions", "confirmation_conditions"]));
    const currentEntryEligibility = ["BLOCKED", "NO_NEW_ENTRY", "RESEARCH_ONLY"].includes(permission ?? "") ? "BLOCKED"
      : eligibility === "DATA_GAP" ? "DATA_GAP"
        : eligibility === "QUALIFIED" && pool === "approved" ? "PENDING_A4" : "NOT_ELIGIBLE";
    const expiry = text(first(value, ["plan_expiry", "expires_at", "expiresAt"]));
    const validFrom = text(first(value, ["valid_from", "validFrom"]));
    const now = safeTime(nowIso);
    const expiryTime = safeTime(expiry);
    const validFromTime = safeTime(validFrom);
    const planId = text(first(value, ["plan_id", "planId"]));
    const validity = !planId ? "NOT_PUBLISHED" : now === null || expiryTime === null ? "TIME_UNVERIFIED"
      : now > expiryTime ? "EXPIRED" : validFromTime !== null && now < validFromTime ? "NOT_YET_VALID" : "VALID_WINDOW";
    base.a3 = {
      dailySetupState: eligibility,
      a4ConfirmationState: currentEntryEligibility === "PENDING_A4" || deferred.length ? "REQUIRED" : "NOT_APPLICABLE",
      currentEntryEligibility,
      planValidityState: validity,
      validFrom,
      expiresAt: expiry,
      deferredConditions: deferred,
      executionPermission: permission,
      target: legacyTarget(value),
    };
  }
  return base;
}

export function normalizePersistedResearchPresentation(value: unknown): ResearchPresentation | null {
  const source = record(value);
  if (source.schema_version !== RESEARCH_PRESENTATION_SCHEMA) return null;
  const stage = text(source.stage);
  const pool = text(source.pool);
  if (!stage || !pool) return null;
  const normalized: ResearchPresentation = {
    schemaVersion: RESEARCH_PRESENTATION_SCHEMA,
    stage,
    pool,
    displayScore: number(source.display_score),
    scoreMeaning: text(source.score_meaning) ?? "NO_INDIVIDUAL_TOTAL_SCORE",
    sourceIdentities: strings(source.source_identities),
    legacyAdapter: source.legacy_adapter === true,
  };
  const a1 = record(source.a1);
  if (Object.keys(a1).length) normalized.a1 = {
    admissionPath: text(a1.admission_path), researchLevel: text(a1.research_level) ?? "UNSPECIFIED",
    permissionMeaning: text(a1.permission_meaning) ?? "RESEARCH_STATE_UNKNOWN",
    criticalGaps: strings(a1.critical_gaps), knownNegativeFacts: strings(a1.known_negative_facts), evidenceAsOf: text(a1.evidence_as_of),
  };
  const a2 = record(source.a2);
  if (Object.keys(a2).length) normalized.a2 = {
    themeStrength: number(a2.theme_strength), stockRelativeStrength: a2.stock_relative_strength ?? null,
    marketRole: a2.market_role ?? null, researchPath: text(a2.research_path), individualTotalScore: number(a2.individual_total_score),
  };
  const a3 = record(source.a3);
  if (Object.keys(a3).length) {
    const target = record(a3.target);
    normalized.a3 = {
      dailySetupState: text(a3.daily_setup_state) ?? "UNKNOWN",
      a4ConfirmationState: text(a3.a4_confirmation_state) ?? "UNKNOWN",
      currentEntryEligibility: text(a3.current_entry_eligibility) ?? "UNKNOWN",
      planValidityState: text(a3.plan_validity_state) ?? "TIME_UNVERIFIED",
      validFrom: text(a3.valid_from), expiresAt: text(a3.expires_at), deferredConditions: strings(a3.deferred_conditions),
      executionPermission: text(a3.execution_permission),
      target: { price: number(target.price), kind: text(target.kind) ?? "UNAVAILABLE", basis: text(target.basis), claim: text(target.claim) ?? "NO_TARGET_EVIDENCE" },
    };
  }
  return normalized;
}

export interface UnifiedPresentationStatusInput {
  jobStatus: string;
  dataState: string;
  opportunityState: string;
  actionabilityState: string;
  criticalData: boolean;
  coverage: number | null;
}

export function unifiedPresentationStatus(input: UnifiedPresentationStatusInput) {
  const job = input.jobStatus.toUpperCase();
  const data = input.dataState.toUpperCase();
  const opportunity = input.opportunityState.toUpperCase();
  const actionability = input.actionabilityState.toUpperCase();
  const overallState = ["FAILED", "TIMED_OUT", "INTERRUPTED", "CANCELLED"].includes(job) ? "JOB_NOT_COMPLETED"
    : input.criticalData && ["INSUFFICIENT", "MISSING", "BLOCKED", "UNKNOWN"].includes(data) ? "BLOCKED_DATA"
      : ["PARTIAL", "DEGRADED"].includes(data) ? opportunity === "ABSENT" ? "SUCCEEDED_DEGRADED_NO_OPPORTUNITY" : "SUCCEEDED_DEGRADED"
        : opportunity === "ABSENT" ? "SUCCEEDED_NO_OPPORTUNITY" : actionability === "ACTIONABLE" ? "ACTIONABLE" : "SUCCEEDED";
  return { ...input, jobStatus: job, dataState: data, opportunityState: opportunity, actionabilityState: actionability, coverage: number(input.coverage), overallState };
}

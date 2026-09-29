"""Read-only schema diagnosis of frozen A5 response/facts; no model or network."""
import argparse
import json
from pathlib import Path
from pydantic import ValidationError
from liangjian_funnel.review.daily import (A5ReviewReport, _canonicalize_report_output, _evidence_ids,
                                         _validate_evidence, _enforce_verified_findings,
                                         _model_fact_projection, _projection_evidence_ids)
from liangjian_funnel.review.fact_guard import reconcile_report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--response", type=Path, required=True)
    parser.add_argument("--facts", type=Path, required=True)
    parser.add_argument("--validate-evidence", action="store_true")
    args = parser.parse_args()
    response = json.loads(args.response.read_text(encoding="utf-8"))
    facts = json.loads(args.facts.read_text(encoding="utf-8"))
    projected_ids = _projection_evidence_ids(_model_fact_projection(facts))
    try:
        report = A5ReviewReport.model_validate(_canonicalize_report_output(response["output"], allowed_evidence=_evidence_ids(facts) | projected_ids))
    except ValidationError as exc:
        print(json.dumps(exc.errors(include_input=False, include_url=False), ensure_ascii=False, default=str))
        raise SystemExit(1)
    if args.validate_evidence:
        _validate_evidence(report, facts, check_stage=False, allowed_projection_evidence=projected_ids)
        _enforce_verified_findings(report, facts)
        report.fact_reconciliation = reconcile_report(report, facts)
        report = A5ReviewReport.model_validate(report.model_dump())
        _validate_evidence(report, facts, allowed_projection_evidence=projected_ids)
        print("ARCHIVED_SCHEMA_EVIDENCE_RECONCILIATION_VALID; NO_MODEL_NO_PUBLISH")
    else:
        print("SCHEMA_VALID_ONLY_NOT_FULL_A5_ACCEPTANCE")


if __name__ == "__main__":
    main()

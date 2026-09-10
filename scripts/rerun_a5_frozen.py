"""Validate or explicitly re-run today's A5 from its immutable failed input."""
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.prompts import PromptRepository
from liangjian_funnel.review.context import render_a5_prompt
from liangjian_funnel.review.daily import (
    A5DailyReviewService, A5ReviewKind, _canonical_hash, _model_fact_projection,
)
from liangjian_funnel.settings import Settings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--facts', type=Path, required=True)
    parser.add_argument('--execute', action='store_true', help='Call the model, persist A5, and send its normal notification')
    parser.add_argument('--model', help='Explicit operator-selected model for this A5 run only; no environment/configuration writes')
    args = parser.parse_args()
    raw = args.facts.read_bytes()
    facts = json.loads(raw)
    now = datetime.now(ZoneInfo('Asia/Shanghai'))
    kind = A5ReviewKind(facts['review_kind'])
    clock = (11, 30) if kind is A5ReviewKind.MIDDAY else (15, 0)
    cutoff = now.replace(hour=clock[0], minute=clock[1], second=0, microsecond=0)
    if (facts['trade_date'] != now.date().isoformat() or facts['cutoff_at'] != cutoff.isoformat()
            or now < cutoff or facts['input_hash'] != _canonical_hash({k:v for k,v in facts.items() if k != 'input_hash'})):
        raise ValueError('A5_FROZEN_FACT_IDENTITY_OR_HASH_MISMATCH')
    settings = Settings.from_env(root=Path.cwd())
    _, diagnostics = render_a5_prompt(PromptRepository(settings.prompt_dir),
        'agent_5_daily_reviewer_v1.txt', _model_fact_projection(facts))
    print(json.dumps({'mode':'execute' if args.execute else 'validate_only',
        'source_file_sha256':hashlib.sha256(raw).hexdigest(), 'context':diagnostics},ensure_ascii=False),flush=True)
    if not args.execute:
        return
    from liangjian_funnel.workflow import WorkflowApplication, _primary_model_for_settings
    app = WorkflowApplication(settings)
    model, _, lane = _primary_model_for_settings(settings)
    model = args.model or settings.review_model
    service = A5DailyReviewService(store=app.store,prompts=app.prompts,
        model_client=app.review_model_client,output_dir=settings.workflow_output_dir,
        lane_id=lane,model=model,notification_publisher=app.lark_publisher)
    result = service.run(review_kind=kind,now=now,frozen_facts=facts)
    print(json.dumps(result,ensure_ascii=False,default=str),flush=True)
    if hashlib.sha256(args.facts.read_bytes()).hexdigest() != hashlib.sha256(raw).hexdigest():
        raise RuntimeError('ORIGINAL_FROZEN_FILE_CHANGED')


if __name__ == '__main__':
    main()

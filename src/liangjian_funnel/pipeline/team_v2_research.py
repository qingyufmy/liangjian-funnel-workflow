"""Team role adapter at the existing research stage boundary.

Explicit constructor injection is required. No global prompt switch, env
mutation, legacy checkpoint reuse, customer publisher or extra data fetch.
A2 additionally requires an explicit approved scoring config; absent that
config its legacy stage remains unchanged. Workflow activation is separate.
"""
from collections.abc import Mapping
from contextlib import closing
from datetime import datetime
import hashlib
import time
from zoneinfo import ZoneInfo

from ..runtime.team_v2_plan_review import plan_review_inputs, run_plan_review
from ..runtime.team_v2_store import canonical
from .team_v2_model import team_call_binding


def _hash(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class TeamResearchAdapter:
    def __init__(self, *, repository, journal, store, calendar, config_version, config_hash,a2_config=None):
        if (not isinstance(config_version, str) or not config_version
                or not isinstance(config_hash, str) or len(config_hash) != 64
                or any(c not in '0123456789abcdef' for c in config_hash)):
            raise ValueError('TEAM_RESEARCH_CONFIG_BINDING_INVALID')
        self.repository = repository
        self.journal = journal
        self.store = store
        self.calendar = calendar
        self.config_version = config_version
        self.config_hash = config_hash
        self.a2_config = a2_config

    def supports(self, stage):
        return stage in {'A1','A3'} or stage=='A2' and self.a2_config is not None

    def prepare_a2_snapshot(self,snapshot,symbols):
        from .team_v2_a2_research import prepare_a2_snapshot
        return prepare_a2_snapshot(self,snapshot,symbols)

    def method_feedback(self,recipient,as_of):
        from .team_v2_feedback import read_a5_method_feedback
        return read_a5_method_feedback(self.journal.path,self.store.path,recipient=recipient,as_of=as_of)

    def finalize_a3_output(self, output):
        # Model ranks are batch-local. Deterministic interleaving is display
        # only, not a claim that one model reviewed the entire merged universe.
        rows = [dict(row) for row in output.get('core_watch_pool', [])]
        for rank, row in enumerate(sorted(rows, key=lambda row: (
                row.get('model_priority_rank', row['plan_priority_rank']), row['symbol'])), 1):
            row.setdefault('model_priority_rank', row['plan_priority_rank'])
            row['plan_priority_rank'] = rank
            row['priority_rank_scope'] = 'SERVER_INTERLEAVED_BATCH_LOCAL_DISPLAY_ONLY'
        return {**output, 'core_watch_pool': rows}

    def _inputs(self, snapshot, upstream, symbols, *, run_id, model):
        stamp = snapshot.as_of
        if not isinstance(stamp, datetime) or stamp.utcoffset() is None:
            raise ValueError('TEAM_RESEARCH_SOURCE_TIME_REQUIRED')
        stamp = stamp.astimezone(ZoneInfo('Asia/Shanghai'))
        source_day = stamp.date()
        if self.calendar.is_trading_day(source_day) is not True or stamp.hour < 15:
            raise ValueError('TEAM_RESEARCH_CLOSED_SOURCE_REQUIRED')
        execution = self.calendar.next_trading_day(source_day).isoformat()
        contexts = snapshot.data.get('A3_DETERMINISTIC_CONTEXT')
        origins = snapshot.data.get('A3_CANDIDATE_ORIGIN')
        if (not isinstance(contexts, Mapping) or not isinstance(origins, Mapping)
                or not symbols <= set(contexts) or not symbols <= set(origins)):
            raise ValueError('TEAM_RESEARCH_A3_SOURCE_DOMAIN_MISSING')
        upstream_rows = {row['symbol']: row for pool in ('focus_pool', 'watch_only_pool')
            for row in upstream.get(pool, []) if isinstance(row, Mapping) and isinstance(row.get('symbol'), str)}
        if not symbols <= set(upstream_rows):
            raise ValueError('TEAM_RESEARCH_A3_UPSTREAM_DOMAIN_MISMATCH')
        domain = {}; engine = {}; facts = {}
        for symbol in sorted(symbols):
            row = contexts[symbol]
            if not isinstance(row, Mapping):
                raise ValueError('TEAM_RESEARCH_A3_SOURCE_DOMAIN_MISSING')
            raw_facts = row.get('strategy_facts')
            if raw_facts is not None and not isinstance(raw_facts, Mapping):
                raise ValueError('TEAM_RESEARCH_A3_FACTS_INVALID')
            origin = origins[symbol]
            cluster = upstream_rows[symbol].get('cluster') or 'UNKNOWN'
            probe_only = (origin == 'WATCH_ONLY' or row.get('plan_mode') == 'PROBE'
                or row.get('risk_unit') == 'PROBE'
                or (raw_facts or {}).get('higher_timeframe_risk') == 'CONDITIONAL_PROBE')
            domain[symbol] = dict(strategy_profile=row.get('strategy_profile'), candidate_origin=origin,
                cluster=cluster, risk_unit='PROBE' if probe_only else 'STANDARD')
            engine[symbol] = dict(eligible=(row.get('eligibility') == 'QUALIFIED'
                and row.get('route_permission') == 'ALLOW_A4' and row.get('execution_permission') != 'BLOCKED'),
                original_context_sha256=_hash(row))
            facts[symbol] = dict(row)
        market = snapshot.data.get('MARKET_REGIME_SNAPSHOT', {'status': 'UNAVAILABLE'})
        values = dict(A3_FACT_SHEET=facts, MARKET_REGIME_SNAPSHOT=market,
            SECTOR_PERMISSIONS=snapshot.data.get('SECTOR_PERMISSIONS', {'status': 'UNAVAILABLE'}),
            A2_HANDOFF=upstream.get('handoff_notes', {'status': 'UNAVAILABLE'}),
            RISK_CAPACITY=snapshot.data.get('RISK_CAPACITY', {'status': 'UNAVAILABLE'}),
            PRIOR_OUTCOME_FEEDBACK=self.method_feedback('A3',stamp))
        projected = plan_review_inputs(values, domain, engine)
        # IDs and hashes refer to the actual blind facts, not engine verdicts.
        items = [dict(id='A3:MARKET', category='MARKET', sha256=_hash(market), summary='冻结市场环境')]
        for symbol in sorted(symbols):
            items.append(dict(id='A3:TECH:' + symbol, category='STOCK_TECH',
                sha256=_hash(projected['A3_FACT_SHEET'][symbol]), summary='冻结月周日技术事实 ' + symbol))
            events = facts[symbol].get('event_calendar')
            if events is not None:
                items.append(dict(id='A3:NEWS:' + symbol, category='NEWS', sha256=_hash(events),
                    summary='冻结事件日历 ' + symbol))
        values['EVIDENCE_CATALOG'] = dict(categories=list(dict.fromkeys(row['category'] for row in items)), items=items)
        envelope = dict(schema_version='team-research-envelope/1', run_id=run_id, stage_id='A3',
            source_trade_date=source_day.isoformat(), execution_trade_date=execution, as_of=stamp.isoformat(),
            knowledge_cutoff=stamp.isoformat(), input_snapshot_ids=[snapshot.snapshot_id],
            config_version=self.config_version, config_hash=self.config_hash, prompt_version='agent-3/4.0.2',
            model_name=model, market_regime=market.get('regime', 'UNKNOWN') if isinstance(market, Mapping) else 'UNKNOWN',
            status='OK', reason_codes=[])
        return values, domain, engine, envelope

    def run_stage(self, pipeline, *, lane_id, model, stage, snapshot, upstream_output,
            upstream_symbols, run_id, projection_symbols=None,bundle=None,a1_discovery_context=None):
        if stage=='A1':
            from .team_v2_a1_research import run_a1_stage
            return run_a1_stage(self,pipeline,lane_id=lane_id,model=model,snapshot=snapshot,
                upstream_output=upstream_output,upstream_symbols=upstream_symbols,bundle=bundle,run_id=run_id,
                projection_symbols=projection_symbols,a1_discovery_context=a1_discovery_context)
        if stage=='A2':
            from .team_v2_a2_research import run_a2_stage
            return run_a2_stage(self,pipeline,lane_id=lane_id,model=model,snapshot=snapshot,
                upstream_output=upstream_output,upstream_symbols=upstream_symbols,bundle=bundle,run_id=run_id,
                projection_symbols=projection_symbols)
        # Lazy import avoids the research package's moved-method bootstrap cycle.
        from .research import common as legacy
        if not self.supports(stage):
            raise ValueError('TEAM_RESEARCH_STAGE_UNSUPPORTED')
        symbols = set(projection_symbols if projection_symbols is not None else upstream_symbols)
        started = time.perf_counter()
        try:
            values, domain, engine, envelope = self._inputs(snapshot, upstream_output or {}, symbols,
                run_id=run_id, model=model)
            timeout = float(pipeline.settings.model_timeout_seconds)
            parent_start = pipeline._deadline_started_monotonic
            if parent_start is not None:
                remaining = float(pipeline.settings.research_close_deadline_seconds) - (time.monotonic() - parent_start)
                timeout = min(timeout, remaining)
            if timeout <= 0:
                return pipeline._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, 'RESEARCH_DEADLINE_EXCEEDED')
            identity = _hash(dict(run_id=run_id, lane_id=lane_id, snapshot_hash=snapshot.snapshot_hash,
                symbols=sorted(symbols), model=model, config_hash=self.config_hash))
            # Request timeout is frozen in the call binding, so a parent deadline
            # must not silently rebind a captured request during process restart.
            context = dict(timeout_seconds=timeout, max_output_tokens=pipeline.settings.model_max_output_tokens)
            context = self.store.freeze_control_context('a3-research-budget:' + identity,
                source_sha256=identity, body=context)
            request = self.repository.prepare_messages('A3', plan_review_inputs(values, domain, engine),
                required_envelope=envelope, session='PLAN_REVIEW')
            if context['timeout_seconds'] > timeout:
                expected = dict(run_id=run_id, role='A3', session='PLAN_REVIEW',
                    source_trade_date=envelope['source_trade_date'], execution_trade_date=envelope['execution_trade_date'],
                    model=model, prompt_hash=request.prompt.sha256, input_hash=request.input_hash,
                    catalog_hash=request.evidence_catalog_hash)
                captured = self.journal.load_captured('A3:PLAN_REVIEW:' + identity, expected=expected)
                failed = self.journal.load_failed('A3:PLAN_REVIEW:' + identity,
                    request_binding=team_call_binding(request, run_id=run_id, model=model,
                        session='PLAN_REVIEW', **context))
                if captured is None and failed is None:
                    return pipeline._blocked_stage(lane_id, model, stage, snapshot.snapshot_id,
                        'TEAM_RESEARCH_REMAINING_BUDGET_INSUFFICIENT')
            result = run_plan_review(repository=self.repository, client=pipeline.model_client, journal=self.journal,
                store=self.store, review_id=identity, call_id='A3:PLAN_REVIEW:' + identity, run_id=run_id, model=model,
                source_trade_date=envelope['source_trade_date'], trade_date=envelope['execution_trade_date'],
                clock=pipeline.now, required_envelope=envelope, replacements=values, candidate_domain=domain,
                engine_decisions=engine, allow_model_call=True, **context)
        except ValueError as exc:
            # Only finite local contract codes are exposed; no provider details.
            safe = str(exc) if str(exc).startswith(('TEAM_', 'A3_', 'CONTROL_', 'MODEL_')) else 'TEAM_RESEARCH_INPUT_INVALID'
            return pipeline._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, safe)
        with closing(self.journal._connect()) as db:
            captured_metrics = db.execute('SELECT latency_ms,provider_attempts FROM model_calls WHERE call_id=?',
                ('A3:PLAN_REVIEW:' + identity,)).fetchone()
        model_latency = captured_metrics[0] if captured_metrics is not None else None
        provider_attempts = captured_metrics[1] if captured_metrics is not None else 0
        diagnostics = dict(team_review_id=identity, legacy_checkpoint_reused=False,
            model_provider_attempts_unknown=captured_metrics is None,
            stage_processing_ms=int((time.perf_counter() - started) * 1000))
        if result['status'] != 'READY':
            return legacy.StageAudit(lane_id, model, stage, 'BLOCKED', snapshot.snapshot_id,
                request.prompt.sha256, request.input_hash, None, model_latency,
                provider_attempts, 'team_v2', (), tuple(result['reason_codes']),
                diagnostics={**diagnostics, 'team_model_status': result['status']})
        report = result['report']
        output = {pool: [dict(row) for row in report[pool]] for pool in (
            'core_watch_pool', 'secondary_watch_pool', 'rejected_candidates')}
        output.update(envelope={**legacy._required_envelope(snapshot, lane_id, model, stage),
            'prompt_version': 'agent-3/4.0.2', 'status': 'OK'}, handoff_notes=report['handoff_notes'],
            what_would_change_my_mind=report['what_would_change_my_mind'],
            analysis_summary={'outcome': 'TEAM_V2_BLIND_PLAN_REVIEW', 'team_review_id': identity,
                'model_comparisons': result['comparisons']})
        output, _ = legacy._canonicalize_stage_lineage(output, stage, upstream_output or {}, snapshot.data)
        output, _, _ = legacy._canonicalize_a3_price_fields(output, snapshot.data)
        output, _ = legacy._apply_a3_candidate_origin_policy(output, snapshot.data)
        output, _ = legacy._apply_stage_threshold_policy(output, stage, snapshot.data)
        output = self.finalize_a3_output(output)
        output = legacy._refresh_analysis_counts(output, stage)
        reasons = legacy._validate_output(output, stage=stage, model=model, snapshot_id=snapshot.snapshot_id,
            upstream_symbols=symbols, snapshot_data=snapshot.data)
        return legacy.StageAudit(lane_id, model, stage, 'BLOCKED' if reasons else 'VALIDATED',
            snapshot.snapshot_id, request.prompt.sha256, request.input_hash, _hash(output),
            model_latency, provider_attempts, 'team_v2',
            tuple(sorted(legacy._approved_symbols(output, stage))) if not reasons else (), tuple(reasons),
            output=output if not reasons else None,
            diagnostics={**diagnostics, 'model_comparisons': result['comparisons']})

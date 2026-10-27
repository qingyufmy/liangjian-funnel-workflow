"""Tony T1=C: extra restrictions, never a substitute for normal risk/PM."""
from decimal import Decimal
import hashlib
import json


def engine_only_geometry(plan, *, entry_pattern_binding, plan_rows):
    row=next((row for row in plan_rows if row.get('plan_id')==plan.plan_id),None)
    raw=None if row is None else row.get('payload_json')
    body=json.loads(raw) if isinstance(raw,str) else {}
    try:
        # With no B overlay the live plan still carries the original range.
        authored_low=Decimal(str(body['trigger_low'])) if body else plan.entry_low
        authored_high=Decimal(str(body['trigger_high'])) if body else plan.entry_high
        authored_chase=Decimal(str(body['no_chase'])) if body else plan.no_chase
        if entry_pattern_binding is not None and not body:
            raise ValueError('original range unavailable')
        if not all(x.is_finite() and x>0 for x in (authored_low,authored_high,authored_chase)):
            raise ValueError('invalid original range')
        low=max(plan.entry_low,authored_low);high=min(plan.entry_high,authored_high)
        chase=min(plan.no_chase,authored_chase)
        return dict(version='engine-only-t1c/1',status='READY',entry_low=str(low),entry_high=str(high),
            no_chase=str(chase),strict_no_chase=True,position_fraction='0.5',
            authored_range=dict(low=str(authored_low),high=str(authored_high)),
            team_range=dict(low=str(plan.entry_low),high=str(plan.entry_high)),
            source_payload_sha256=hashlib.sha256(raw.encode()).hexdigest() if raw else None,
            pattern_binding_sha256=entry_pattern_binding.get('binding_sha256') if entry_pattern_binding else None)
    except (ValueError,KeyError,TypeError,ArithmeticError):
        return dict(version='engine-only-t1c/1',status='UNAVAILABLE',position_fraction='0.5',strict_no_chase=True)


def restriction_reason(policy,plan):
    if policy.get('status')!='READY':return 'ENGINE_ONLY_GEOMETRY_UNPROVEN'
    if not Decimal(policy['entry_low'])<=plan.price<=Decimal(policy['entry_high']):
        return 'ENGINE_ONLY_OUTSIDE_STRICT_ENTRY_RANGE'
    if plan.price>=Decimal(policy['no_chase']):return 'ENGINE_ONLY_NO_CHASE'
    return None


def half_position_quantity(normal_additional, owned_quantity):
    """50% of the normally permitted TOTAL position; existing lots consume it.

    Reservations are included in owned_quantity. Rounding only goes down;
    neither a minimum board lot nor an extra batch is invented.
    """
    normal_total=owned_quantity+normal_additional
    target_total=int(Decimal(normal_total)*Decimal('0.5')//100)*100
    additional=max(0,(target_total-owned_quantity)//100*100)
    return min(normal_additional,additional),dict(normal_additional=normal_additional,
        owned_and_reserved_quantity=owned_quantity,normal_total_quantity=normal_total,
        half_total_cap=target_total,additional_quantity=additional)

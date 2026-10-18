"""0253 review wake-up ratios, not position/risk limits or entry permission."""
from decimal import Decimal
import hashlib
from pathlib import Path


KEYS={'version','approved_by','daily_loss_review_at','cluster_exposure_review_at',
    'position_stop_proximity','drawdown_review_at','max_reviews_per_object_type_day'}


def validate_exception_policy(value):
    if (not isinstance(value,dict) or set(value)!=KEYS or value['version']!='exception-triggers/1'
            or value['approved_by']!='claude' or type(value['max_reviews_per_object_type_day']) is not int
            or value['max_reviews_per_object_type_day']!=2):raise ValueError('EXCEPTION_TRIGGER_POLICY_INVALID')
    try:
        for key in KEYS-{'version','approved_by','max_reviews_per_object_type_day'}:
            if isinstance(value[key],bool):raise ValueError
            amount=Decimal(str(value[key]))
            if not amount.is_finite() or not 0<amount<=1:raise ValueError
    except (ValueError,ArithmeticError):raise ValueError('EXCEPTION_TRIGGER_POLICY_INVALID') from None
    return dict(value)


def load_exception_policy(path):
    import yaml
    raw=Path(path).read_bytes();policy=validate_exception_policy(yaml.safe_load(raw)['exception_triggers'])
    return dict(policy,config_sha256=hashlib.sha256(raw).hexdigest())

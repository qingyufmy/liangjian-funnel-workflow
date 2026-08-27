"""Offline evaluation helpers for the A-share research funnel.

Evaluation datasets are deliberately kept outside the runtime pipeline.  In
particular, the broker monthly ``gold`` list exported here is an audit
benchmark only; importing it cannot add symbols to any research pool.
"""

from .broker_gold import (
    BROKER_GOLD_SCHEMA_VERSION,
    BROKER_GOLD_FIELDS,
    BrokerGoldContractError,
    BrokerGoldDataset,
    BrokerGoldRecord,
    evaluate_broker_gold,
    import_broker_gold,
    load_broker_gold_csv,
    load_broker_gold_json,
    normalize_broker_gold_rows,
)

__all__ = [
    "BROKER_GOLD_SCHEMA_VERSION",
    "BROKER_GOLD_FIELDS",
    "BrokerGoldContractError",
    "BrokerGoldDataset",
    "BrokerGoldRecord",
    "evaluate_broker_gold",
    "import_broker_gold",
    "load_broker_gold_csv",
    "load_broker_gold_json",
    "normalize_broker_gold_rows",
]

"""0417 model-only nomination evidence labels; no raw/validator ref aliases.

The frozen fact and catalog keep their original bytes/hash. Labels are server
transport metadata and their inverse is explicit and checked before HTTP.
"""
from collections.abc import Mapping
from copy import deepcopy
import hashlib
import json
import re

_ANNOTATIONS = {'evidence_ref', 'evidence_role', 'source_fact_sha256'}
_ROLE = 'FILTERED_NOMINATION_ONLY'


def _sha(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _catalog(catalog):
    if not isinstance(catalog, Mapping) or not isinstance(catalog.get('items'), list):
        raise ValueError('TEAM_A2_NOMINATION_EVIDENCE_CATALOG_INVALID')
    result = {}
    for item in catalog['items']:
        if not isinstance(item, Mapping) or not isinstance(item.get('id'), str) or item['id'] in result:
            raise ValueError('TEAM_A2_NOMINATION_EVIDENCE_CATALOG_INVALID')
        result[item['id']] = item
    return result


def bind_nomination_evidence(sample, catalog):
    """Annotate a copy, while catalog SHA continues to identify the original row."""
    if not isinstance(sample, list):
        raise ValueError('TEAM_A2_NOMINATION_EVIDENCE_SOURCE_INVALID')
    known = _catalog(catalog)
    result = []
    seen = set()
    for row in sample:
        if (not isinstance(row, Mapping) or not isinstance(row.get('symbol'), str)
                or not row['symbol'] or row['symbol'] in seen or _ANNOTATIONS.intersection(row)):
            raise ValueError('TEAM_A2_NOMINATION_EVIDENCE_SOURCE_INVALID')
        seen.add(row['symbol'])
        identity = 'A2:NOMINATION:' + row['symbol']
        source_hash = _sha(row)
        entry = known.get(identity)
        if (entry is None or entry.get('sha256') != source_hash or entry.get('category') != 'QUANT'
                or 'A2:QUANT:' + row['symbol'] in known):
            raise ValueError('TEAM_A2_NOMINATION_EVIDENCE_BINDING_MISMATCH')
        result.append(dict(deepcopy(dict(row)), evidence_ref=identity,
            evidence_role=_ROLE, source_fact_sha256=source_hash))
    return result


def strip_nomination_evidence(sample, catalog):
    """Verify every annotation and remove only these server-owned metadata keys."""
    if not isinstance(sample, list):
        raise ValueError('TEAM_A2_NOMINATION_EVIDENCE_SOURCE_INVALID')
    original = []
    for row in sample:
        if not isinstance(row, Mapping) or not _ANNOTATIONS.issubset(row):
            raise ValueError('TEAM_A2_NOMINATION_EVIDENCE_BINDING_MISMATCH')
        original.append({key: deepcopy(value) for key, value in row.items() if key not in _ANNOTATIONS})
    if bind_nomination_evidence(original, catalog) != sample:
        raise ValueError('TEAM_A2_NOMINATION_EVIDENCE_BINDING_MISMATCH')
    return original


def prepare_nomination_evidence_values(values):
    """Project only model input; the caller freezes unannotated values unchanged."""
    projected = deepcopy(dict(values))
    sample = values['A2_QUANT_FILTERED_SAMPLE']
    catalog = values['EVIDENCE_CATALOG']
    annotated = bind_nomination_evidence(sample, catalog)
    projected['A2_QUANT_FILTERED_SAMPLE'] = annotated
    restored = deepcopy(projected)
    restored['A2_QUANT_FILTERED_SAMPLE'] = strip_nomination_evidence(annotated, catalog)
    if restored != values:
        raise ValueError('TEAM_A2_NOMINATION_EVIDENCE_INVERSE_MISMATCH')
    return projected, dict(schema='a2-nomination-evidence-labels/1',
        original_values_sha256=_sha(values), model_values_sha256=_sha(projected),
        source_sample_sha256=_sha(sample), projected_sample_sha256=_sha(annotated),
        original_catalog_sha256=_sha(catalog), original_catalog_unchanged=projected['EVIDENCE_CATALOG'] == catalog,
        metadata_keys=sorted(_ANNOTATIONS), inverse='VERIFY_BINDINGS_THEN_STRIP_ONLY_METADATA_KEYS',
        inverse_original_values_exact=True,
        bindings=[dict(symbol=row['symbol'], evidence_ref=row['evidence_ref'], evidence_role=row['evidence_role'],
            source_fact_sha256=row['source_fact_sha256'], projected_row_sha256=_sha(row)) for row in annotated])


def same_fact_namespace_suggestions(raw, catalog, issues, nomination_sample):
    """Suggest a real fact ID to the next answer, never rewrite the old answer.

    No guessed ticker, reverse namespace, missing fact, changed SHA, duplicate
    source row or already-existing QUANT ID can authorize a suggestion.
    """
    if not isinstance(raw, Mapping):
        return []
    try:
        if any(_ANNOTATIONS.intersection(row) for row in nomination_sample):
            originals = strip_nomination_evidence(nomination_sample, catalog)
        else:
            originals = deepcopy(nomination_sample)
        annotated = bind_nomination_evidence(originals, catalog)
        known = _catalog(catalog)
    except (TypeError, KeyError, ValueError):
        return []
    source = {row['symbol']: row for row in annotated}
    suggestions = []
    for issue in issues:
        match = re.fullmatch(r'TEAM_(SUPPORTING|OPPOSING)_REFS_INVALID:([a-z_]+)\[(\d+)\]', issue)
        if not match:
            continue
        field = match[1].lower() + '_refs'
        try:
            refs = raw[match[2]][int(match[3])][field]
        except (KeyError, IndexError, TypeError):
            continue
        if not isinstance(refs, list):
            continue
        for invalid in refs:
            if not isinstance(invalid, str) or not invalid.startswith('A2:QUANT:') or invalid in known:
                continue
            row = source.get(invalid.removeprefix('A2:QUANT:'))
            if row is None:
                continue
            suggestions.append(dict(field=f'{match[2]}[{match[3]}].{field}',
                invalid_reference_id=invalid, legal_reference_id=row['evidence_ref'],
                source_fact_sha256=row['source_fact_sha256'], evidence_role=row['evidence_role'],
                purpose='REFERENCE_ONLY_NOT_NOMINATION_PROMOTION_OR_EXECUTION_PERMISSION',
                provider_capture_unchanged=True))
    return suggestions

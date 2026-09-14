"""Bounded reuse of complete, content-validated reference graph versions."""

from datetime import datetime, timedelta


def load_recent_reference(cache_path, *, as_of, max_age_days, loader):
    if type(max_age_days) is not int or not 0 <= max_age_days <= 7:
        raise ValueError("reference cache max age must be between 0 and 7 days")
    prefix = cache_path.name[:-15]  # strip YYYY-MM-DD.json
    for age in range(max_age_days + 1):
        day = (as_of.date() - timedelta(days=age)).isoformat()
        path = cache_path.with_name(f"{prefix}{day}.json")
        cached = loader(path, day)
        if cached is None:
            continue
        try:
            fetched = datetime.fromisoformat(cached["fetched_at"])
            if fetched.tzinfo is None or fetched > as_of or (as_of - fetched).total_seconds() > (max_age_days + 1) * 86400:
                continue
        except (KeyError, TypeError, ValueError):
            continue
        return cached
    return None

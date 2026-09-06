"""Refresh named slow-changing board memberships without changing daily rankings."""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.data.rotation_theme import (
    EASTMONEY_BOARD_SOURCE_ID, build_membership_snapshot,
    collect_eastmoney_board_members, load_rotation_theme_config, write_membership_snapshot,
    collect_eastmoney_board_catalog, validate_board_identity,
)
from liangjian_funnel.reporting import atomic_write_json


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--theme", action="append")
    group.add_argument("--all", action="store_true")
    parser.add_argument("--registry")
    parser.add_argument("--catalog-output")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    config = load_rotation_theme_config(args.registry)
    catalog = collect_eastmoney_board_catalog(as_of=datetime.now(ZoneInfo("Asia/Shanghai")))
    if catalog.get("available") is not True:
        raise SystemExit("MEMBERSHIP_REFRESH_CATALOG_UNAVAILABLE")
    if args.catalog_output:
        atomic_write_json(Path(args.catalog_output), catalog)
    for theme_id in ([theme.theme_id for theme in config.themes] if args.all else args.theme):
        theme = config.get(theme_id)
        if error := validate_board_identity(theme, catalog):
            raise SystemExit(f"{error}:{theme_id}")
        if not theme.eastmoney_board_codes:
            raise SystemExit(f"MEMBERSHIP_REFRESH_UNRESOLVED:{theme_id}")
        stamp = datetime.now(ZoneInfo("Asia/Shanghai"))
        records, pages, captures = {}, [], []
        for code in theme.eastmoney_board_codes:
            result = collect_eastmoney_board_members(as_of=stamp, board_code=code)
            if result.get("available") is not True:
                raise SystemExit(f"MEMBERSHIP_REFRESH_FAILED:{theme_id}:{result.get('reason_code')}")
            records.update({row["symbol"]: row for row in result["records"]})
            pages.append({"board_code": code, "board_name": theme.eastmoney_board_names.get(code), "provider_total": result["provider_total"],
                          "pagination_evidence": result["pagination_evidence"]})
            captures.append(datetime.fromisoformat(result["captured_at"]))
        snapshot = build_membership_snapshot(
            theme_id=theme_id, members=list(records.values()), captured_at=max(captures),
            effective_from=stamp.date(), source=EASTMONEY_BOARD_SOURCE_ID,
            pagination_evidence={"total": len(records), "page_size": None, "pages": pages, "complete": True},
        )
        path = write_membership_snapshot(args.output_dir, snapshot)
        print(json.dumps({"theme_id": theme_id, "member_count": snapshot["member_count"],
                          "captured_at": snapshot["captured_at"], "path": str(path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

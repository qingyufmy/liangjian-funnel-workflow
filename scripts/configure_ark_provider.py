"""Probe an explicitly supplied Ark key; optionally provision the local .env.

Read the secret from stdin, never argv. No key or provider error body is logged.
Run from the intended project root, as its existing owner.
"""
import argparse
import json
import os
from pathlib import Path
import stat
import sys

from liangjian_funnel.pipeline.model_client import ModelClientError, OpenAICompatibleModelClient
from liangjian_funnel.reporting import atomic_write_text
from liangjian_funnel.settings import Settings, load_dotenv

BASE_URL = "https://ark.cn-beijing.volces.com/api/plan/v3"
MODEL = "deepseek-v4-pro"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--use-configured-key", action="store_true", help="Use the key already in the project .env; do not read stdin")
    args = parser.parse_args()
    path = Path.cwd() / ".env"
    if not path.is_file() or path.is_symlink():
        raise SystemExit("PROJECT_ENV_MISSING_OR_SYMLINK")
    before = path.read_text(encoding="utf-8-sig")
    key = load_dotenv(path).get("LIANGJIAN_MODEL_API_KEY", "") if args.use_configured_key else sys.stdin.readline().strip()
    if not key or any(ch.isspace() for ch in key):
        raise SystemExit("ARK_KEY_INPUT_INVALID")
    updates = {"LIANGJIAN_MODEL_BASE_URL": BASE_URL, "LIANGJIAN_MODEL_API_KEY": key,
        "LIANGJIAN_RESEARCH_MODEL": MODEL, "LIANGJIAN_REVIEW_MODEL": MODEL,
        "LIANGJIAN_MONITOR_MODEL": MODEL}
    configured = Settings.from_env({**load_dotenv(path), **updates}, root=Path.cwd())
    probe_settings = configured.model_copy(update={"model_timeout_seconds": 40,
        "model_max_output_tokens": 1024, "model_fallback_output_tokens": 1024,
        "model_secondary_fallback_output_tokens": 1024})
    client = OpenAICompatibleModelClient(probe_settings, max_attempts=1)
    for stage in ("A4", "A5"):
        try:
            result = client.complete(MODEL, [{"role": "user", "content": 'Return exactly JSON: {"ok":true}'}],
                stage=stage, timeout_seconds=40, max_output_tokens=1024)
        except ModelClientError as exc:
            print(json.dumps({"status": "FAILED", "stage": stage, "reason_code": exc.reason_code,
                "http_status": exc.status_code, "configuration_changed": False}), flush=True)
            raise SystemExit(2)
        if result.output != {"ok": True}:
            raise SystemExit("ARK_PROBE_OUTPUT_INVALID")
        print(json.dumps({"status": "PASSED", "stage": stage, "model": MODEL,
            "latency_ms": result.latency_ms, "thinking_variant": result.thinking_variant}), flush=True)
    if args.apply:
        if path.read_text(encoding="utf-8-sig") != before:
            raise SystemExit("ENV_CHANGED_DURING_PROBE")
        existing_mode = stat.S_IMODE(path.stat().st_mode)
        lines = []
        for line in before.splitlines():
            name = line.strip().removeprefix("export ").split("=", 1)[0].strip()
            if name not in updates:
                lines.append(line)
        lines.extend(f"{name}={value}" for name, value in updates.items())
        atomic_write_text(path, "\n".join(lines) + "\n")
        os.chmod(path, existing_mode & 0o660)
        verified = Settings.from_env(load_dotenv(path), root=Path.cwd())
        if verified.model_base_url != BASE_URL or verified.research_models != (MODEL,) or verified.review_model != MODEL or verified.monitor_model != MODEL:
            raise SystemExit("ARK_ENV_VERIFICATION_FAILED")
    print(json.dumps({"status": "READY", "configuration_changed": args.apply,
        "base_url": BASE_URL, "research_model": MODEL, "review_model": MODEL, "monitor_model": MODEL}), flush=True)


if __name__ == "__main__":
    main()

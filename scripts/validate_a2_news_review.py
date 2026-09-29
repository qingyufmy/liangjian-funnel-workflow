"""Validate a saved shadow model response offline; never calls a model."""
import argparse
import json
from pathlib import Path

from liangjian_funnel.pipeline.a2_news_review import validate_review


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--response", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output exists; original input/response must remain immutable")
    frozen = json.loads(args.input.read_text(encoding="utf-8"))
    packet = frozen.get("review_input", frozen)
    response = json.loads(args.response.read_text(encoding="utf-8"))
    result = validate_review(response, packet)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
    print("CITATION_CONTRACT_VALID; SHADOW_ONLY; SEMANTIC_TRUTH_NOT_VERIFIED")


if __name__ == "__main__":
    main()

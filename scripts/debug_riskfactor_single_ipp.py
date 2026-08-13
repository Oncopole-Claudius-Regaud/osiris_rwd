#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict
from pathlib import Path

from extract_riskfactor_by_ipp import (
    extract_hits_for_document,
    extract_pdf_text,
    is_consultation_document,
    load_metadata,
    metadata_date,
    metadata_ipp,
    metadata_pdf_path,
    riskfactor_scope,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Debug riskFactor regex matches for one IPP.")
    parser.add_argument("--source-dir", default="/opt/PDF")
    parser.add_argument("--ipp", required=True)
    parser.add_argument("--all-documents", action="store_true")
    parser.add_argument("--show-scope", action="store_true")
    parser.add_argument("--jsonl-output")
    return parser.parse_args()


def compact(value: str, limit: int = 1200) -> str:
    value = re.sub(r"\s+", " ", value).strip()
    return value[:limit] + ("..." if len(value) > limit else "")


def main() -> int:
    args = parse_args()
    source_dir = Path(args.source_dir)
    output_handle = open(args.jsonl_output, "w", encoding="utf-8") if args.jsonl_output else None
    docs_seen = 0
    docs_selected = 0
    hits_total = 0

    try:
        for metadata_path in sorted(source_dir.glob("*.json.txt")):
            try:
                metadata = load_metadata(metadata_path)
            except Exception:
                continue
            if metadata_ipp(metadata, metadata_path) != args.ipp:
                continue
            docs_seen += 1
            consult = is_consultation_document(metadata)
            if not args.all_documents and not consult:
                continue

            pdf_path = metadata_pdf_path(metadata_path)
            print(f"\nDOCUMENT | consult={consult} | date={metadata_date(metadata)} | pdf={pdf_path.name}")
            if not pdf_path.exists():
                print("  missing_pdf")
                continue
            docs_selected += 1
            try:
                text = extract_pdf_text(pdf_path)
            except Exception as exc:
                print(f"  unreadable_pdf={exc}")
                continue

            scope = riskfactor_scope(text)
            if args.show_scope:
                print(f"  scope={compact(scope)}")

            hits = extract_hits_for_document(args.ipp, pdf_path, metadata_date(metadata), text)
            if not hits:
                print("  no_match")
                continue

            for hit in hits:
                hits_total += 1
                print(
                    "  MATCH | "
                    f"type={hit.riskfactortype} | value={hit.riskfactorvalue} | "
                    f"pathogen={hit.pathogen or ''} | confidence={hit.confidence}"
                )
                print(f"    text={compact(hit.matched_text, 500)}")
                if output_handle:
                    output_handle.write(json.dumps(asdict(hit), ensure_ascii=False) + "\n")

        print(f"\nSUMMARY | ipp={args.ipp} | docs_seen={docs_seen} | docs_selected={docs_selected} | hits={hits_total}")
    finally:
        if output_handle:
            output_handle.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

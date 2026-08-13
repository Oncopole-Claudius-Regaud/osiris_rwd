#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from extract_riskfactor_by_ipp import (
    is_consultation_document,
    load_metadata,
    metadata_ipp,
    metadata_pdf_path,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Copy consultation PDF/metadata files for one IPP.")
    parser.add_argument("--source-dir", default="/opt/PDF")
    parser.add_argument("--target-dir", default="/opt/pdf_test")
    parser.add_argument("--ipp", required=True)
    parser.add_argument("--clean-target", action="store_true")
    parser.add_argument("--all-documents", action="store_true")
    return parser.parse_args()


def clean_dir(path: Path) -> None:
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_file() or child.is_symlink():
            child.unlink()
        elif child.is_dir():
            shutil.rmtree(child)


def main() -> int:
    args = parse_args()
    source_dir = Path(args.source_dir)
    target_dir = Path(args.target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    if args.clean_target:
        clean_dir(target_dir)

    copied_json = 0
    copied_pdf = 0
    skipped_not_consult = 0
    missing_pdf = 0

    for metadata_path in sorted(source_dir.glob("*.json.txt")):
        try:
            metadata = load_metadata(metadata_path)
        except Exception:
            continue
        if metadata_ipp(metadata, metadata_path) != args.ipp:
            continue
        if not args.all_documents and not is_consultation_document(metadata):
            skipped_not_consult += 1
            continue

        shutil.copy2(metadata_path, target_dir / metadata_path.name)
        copied_json += 1
        pdf_path = metadata_pdf_path(metadata_path)
        if pdf_path.exists():
            shutil.copy2(pdf_path, target_dir / pdf_path.name)
            copied_pdf += 1
        else:
            missing_pdf += 1

    print(f"ipp={args.ipp}")
    print(f"source_dir={source_dir}")
    print(f"target_dir={target_dir}")
    print(f"copied_json={copied_json}")
    print(f"copied_pdf={copied_pdf}")
    print(f"missing_pdf={missing_pdf}")
    print(f"skipped_not_consult={skipped_not_consult}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

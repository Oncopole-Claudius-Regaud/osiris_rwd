#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from collections import Counter
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit PDF metadata labels used to identify consultation reports."
    )
    parser.add_argument("--source-dir", default="/opt/PDF")
    parser.add_argument("--limit", type=int, default=100)
    return parser.parse_args()


def normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(char for char in value if not unicodedata.combining(char))
    value = value.replace("’", "'").replace("`", "'")
    return re.sub(r"\s+", " ", value).strip()


def load_metadata(path: Path) -> dict | None:
    raw = path.read_bytes()
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return json.loads(raw.decode(encoding))
        except Exception:
            continue
    return None


def document_fields(metadata: dict) -> tuple[str, str, str]:
    doc = metadata.get("Document") or {}
    return (
        str(doc.get("TypeDescription") or "").strip(),
        str(doc.get("FormatComDesc") or "").strip(),
        str(doc.get("PrescriptionDesc") or "").strip(),
    )


def old_consultation_rule(type_desc: str, format_desc: str, prescription_desc: str) -> bool:
    haystack = normalize_text(" | ".join([type_desc, format_desc, prescription_desc])).lower()
    if re.search(r"\b(anapath|anatomopath|histolog|cytolog|ordonnance|certificat|anesthesie|consentement)\b", haystack):
        return False
    return bool(
        re.search(r"\bconsultation\b", haystack)
        or re.search(r"\bcr\s+consult", haystack)
        or re.search(r"\bcompte\s+rendu\s+de\s+consult", haystack)
    )


def cs_generales_rule(type_desc: str, format_desc: str, prescription_desc: str) -> bool:
    haystack = normalize_text(" | ".join([type_desc, format_desc, prescription_desc])).lower()
    if re.search(r"\b(anapath|anatomopath|histolog|cytolog|ordonnance|certificat|anesthesie|consentement)\b", haystack):
        return False
    return bool(
        re.search(r"\bconsultation\b", haystack)
        or re.search(r"\bcr\s+consult", haystack)
        or re.search(r"\bcompte\s+rendu\s+de\s+consult", haystack)
        or re.search(r"\bcs\s+generales?\b", haystack)
    )


def main() -> int:
    args = parse_args()
    source_dir = Path(args.source_dir)
    metadata_files = sorted(source_dir.glob("*.json.txt"))

    labels = Counter()
    old_kept = Counter()
    cs_kept = Counter()
    total_decoded = 0

    for metadata_path in metadata_files:
        metadata = load_metadata(metadata_path)
        if metadata is None:
            continue
        total_decoded += 1
        fields = document_fields(metadata)
        labels[fields] += 1
        if old_consultation_rule(*fields):
            old_kept[fields] += 1
        if cs_generales_rule(*fields):
            cs_kept[fields] += 1

    print(f"metadata_files={len(metadata_files)}")
    print(f"decoded_metadata={total_decoded}")
    print(f"old_rule_kept={sum(old_kept.values())}")
    print(f"cs_generales_rule_kept={sum(cs_kept.values())}")
    print()
    print("Top labels:")
    for (type_desc, format_desc, prescription_desc), count in labels.most_common(args.limit):
        old_flag = "OLD_KEEP" if old_consultation_rule(type_desc, format_desc, prescription_desc) else "old_skip"
        cs_flag = "CS_KEEP" if cs_generales_rule(type_desc, format_desc, prescription_desc) else "cs_skip"
        print(
            f"{count}\t{old_flag}\t{cs_flag}\t"
            f"Type={type_desc}\tFormat={format_desc}\tPrescription={prescription_desc}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

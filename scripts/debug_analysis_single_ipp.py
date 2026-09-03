#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from extract_analysis_by_ipp import (
    analysis_in_center,
    analysis_type_from_metadata,
    document_fields,
    load_metadata,
    metadata_code,
    metadata_date,
    metadata_ipp,
    normalize_label,
    split_date,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Debug OSIRIS RWD analysis metadata matching for one IPP."
    )
    parser.add_argument("--source-dir", default="/opt/PDF")
    parser.add_argument("--ipp", required=True)
    parser.add_argument("--only-matches", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_dir = Path(args.source_dir)
    target_ipp = str(args.ipp).strip()

    docs_seen = 0
    docs_patient = 0
    matches = 0
    unreadable = 0

    for metadata_path in sorted(source_dir.rglob("*.json.txt")):
        docs_seen += 1
        try:
            metadata = load_metadata(metadata_path)
        except Exception as exc:
            unreadable += 1
            if not args.only_matches:
                print(f"UNREADABLE | metadata={metadata_path.name} | error={exc}")
            continue

        ipp = metadata_ipp(metadata, metadata_path)
        if ipp != target_ipp:
            continue

        docs_patient += 1
        type_desc, format_desc, prescription_desc = document_fields(metadata)
        analysistype, matched_label = analysis_type_from_metadata(metadata)
        source_date = metadata_date(metadata)
        day, month, year = split_date(source_date)

        if analysistype:
            matches += 1
            status = "MATCH"
        else:
            status = "NO_MATCH"

        if args.only_matches and not analysistype:
            continue

        print("")
        print(f"{status} | metadata={metadata_path.name}")
        print(f"  ipp={ipp}")
        print(f"  TypeDescription={type_desc}")
        print(f"  FormatComDesc={format_desc}")
        print(f"  PrescriptionDesc={prescription_desc}")
        print(f"  normalized={normalize_label(matched_label)}")
        print(f"  analysistype={analysistype or ''}")
        print(f"  analysiscode={metadata_code(metadata, metadata_path) or ''}")
        print(f"  analysisincenter={analysis_in_center(metadata, metadata_path)}")
        print(f"  analysisdate={source_date or ''} ({day or ''}/{month or ''}/{year or ''})")

    print("")
    print(
        "SUMMARY | "
        f"ipp={target_ipp} | metadata_seen={docs_seen} | patient_docs={docs_patient} | "
        f"matches={matches} | unreadable={unreadable}"
    )

    if docs_patient == 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

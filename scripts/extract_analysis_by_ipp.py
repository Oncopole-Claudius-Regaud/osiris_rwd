#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import re
import unicodedata
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


LOGGER = logging.getLogger("osiris_rwd_analysis")

ANALYSIS_IMAGING = "OSIRISRWD:C37-2"
ANALYSIS_SEQUENCING = "OSIRISRWD:C37-3"


@dataclass
class AnalysisHit:
    patientid: str
    analysistype: str
    analysiscode: Optional[str]
    analysisincenter: Optional[bool]
    analysisdateday: Optional[int]
    analysisdatemonth: Optional[int]
    analysisdateyear: Optional[int]
    source_metadata: str
    source_date: Optional[str]
    matched_label: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract OSIRIS RWD analysis rows from PDF metadata sidecars."
    )
    parser.add_argument("--source-dir", default="/opt/PDF")
    parser.add_argument("--ipp-file", required=True)
    parser.add_argument("--output-dir", default="/opt/extract_osiris_rwd/output")
    parser.add_argument("--jsonl-name", default="analysis_results.jsonl")
    parser.add_argument("--progress-every", type=int, default=10000)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="[%(asctime)s] %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def normalize_label(value: str) -> str:
    value = strip_accents(value).lower()
    value = value.replace("�", " ")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def load_ipps(path: Path) -> set[str]:
    raw = path.read_text(encoding="utf-8", errors="replace").strip()
    if not raw:
        return set()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {token.strip() for token in re.split(r"[,;\r\n]+", raw) if token.strip()}
    if isinstance(payload, list):
        return {str(value).strip() for value in payload if str(value).strip()}
    if isinstance(payload, dict):
        values = payload.get("ipp_list") or payload.get("ipps") or []
        return {str(value).strip() for value in values if str(value).strip()}
    raise ValueError("Unsupported IPP file format")


def load_metadata(path: Path) -> dict:
    raw = path.read_bytes()
    last_error: Optional[Exception] = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return json.loads(raw.decode(encoding))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            last_error = exc
    raise ValueError(f"Unable to decode {path}: {last_error}")


def metadata_ipp(metadata: dict, path: Path) -> str:
    ipp = (metadata.get("Patient") or {}).get("IPP") or metadata.get("IPP")
    if ipp is None:
        ipp = path.name.split("_")[0]
    return str(ipp).strip()


def metadata_date(metadata: dict) -> Optional[str]:
    doc = metadata.get("Document") or {}
    episode = metadata.get("Episode") or {}
    for raw in (
        doc.get("CreateDate"),
        doc.get("UpdateDate"),
        doc.get("DocumentDate"),
        episode.get("StartDate"),
    ):
        token = str(raw or "").strip()
        digits = re.sub(r"\D", "", token)
        if len(digits) >= 8:
            try:
                return datetime.strptime(digits[:8], "%Y%m%d").date().isoformat()
            except ValueError:
                continue
        if len(token) >= 10:
            try:
                return datetime.fromisoformat(token[:10]).date().isoformat()
            except ValueError:
                continue
    return None


def split_date(value: Optional[str]) -> tuple[Optional[int], Optional[int], Optional[int]]:
    if not value:
        return None, None, None
    try:
        parsed = datetime.strptime(value[:10], "%Y-%m-%d").date()
    except ValueError:
        return None, None, None
    return parsed.day, parsed.month, parsed.year


def document_fields(metadata: dict) -> tuple[str, str, str]:
    doc = metadata.get("Document") or {}
    return (
        str(doc.get("TypeDescription") or ""),
        str(doc.get("FormatComDesc") or ""),
        str(doc.get("PrescriptionDesc") or ""),
    )


def metadata_code(metadata: dict, metadata_path: Path) -> Optional[str]:
    doc = metadata.get("Document") or {}
    episode = metadata.get("Episode") or {}
    for raw in (
        doc.get("DocumentId"),
        doc.get("DocumentID"),
        doc.get("DocId"),
        doc.get("DocID"),
        doc.get("Id"),
        doc.get("ID"),
        episode.get("EpisodeNumber"),
        episode.get("EpisodeId"),
        episode.get("EpisodeID"),
    ):
        value = str(raw or "").strip()
        if value:
            return value

    name = metadata_path.name
    if name.lower().endswith(".json.txt"):
        name = name[:-9]
    return Path(name).stem or None


def analysis_type_from_metadata(metadata: dict) -> tuple[Optional[str], str]:
    type_desc, format_desc, prescription_desc = document_fields(metadata)
    label = " | ".join(part for part in (type_desc, format_desc, prescription_desc) if part)
    haystack = normalize_label(label)

    if re.search(r"\bbiologie moleculaire\b", haystack):
        return ANALYSIS_SEQUENCING, label

    if re.search(
        r"\b("
        r"scanner|irm|radiologie|radio senologie|echographie|"
        r"imagerie interventionnelle|cr imagerie|tepscan|tep scan|tep|"
        r"medecine nucleaire|nucleaire|fraction eject(?:ion)? ventriculaire"
        r")\b",
        haystack,
    ):
        if re.search(r"\bconsentement imagerie\b", haystack):
            return None, label
        return ANALYSIS_IMAGING, label

    if re.search(r"\bcr d imagerie medicale\b", haystack):
        return ANALYSIS_IMAGING, label

    return None, label


def analysis_in_center(metadata: dict, metadata_path: Path) -> Optional[bool]:
    type_desc, _, prescription_desc = document_fields(metadata)
    label = normalize_label(" ".join([type_desc, prescription_desc, metadata_path.name]))
    if re.search(r"\b(iuct|icr|institut claudius regaud|oncopole)\b", label):
        return True
    return None


def iter_analysis_hits(source_dir: Path, patientids: set[str]) -> tuple[list[AnalysisHit], int, int]:
    hits: list[AnalysisHit] = []
    unreadable = 0
    seen = 0

    for metadata_path in sorted(source_dir.rglob("*.json.txt")):
        seen += 1
        try:
            metadata = load_metadata(metadata_path)
        except ValueError as exc:
            unreadable += 1
            LOGGER.warning("Unreadable metadata skipped | file=%s | error=%s", metadata_path, exc)
            continue

        ipp = metadata_ipp(metadata, metadata_path)
        if ipp not in patientids:
            continue

        analysistype, matched_label = analysis_type_from_metadata(metadata)
        if not analysistype:
            continue

        source_date = metadata_date(metadata)
        day, month, year = split_date(source_date)
        hits.append(
            AnalysisHit(
                patientid=ipp,
                analysistype=analysistype,
                analysiscode=metadata_code(metadata, metadata_path),
                analysisincenter=analysis_in_center(metadata, metadata_path),
                analysisdateday=day,
                analysisdatemonth=month,
                analysisdateyear=year,
                source_metadata=metadata_path.name,
                source_date=source_date,
                matched_label=matched_label,
            )
        )

    return hits, seen, unreadable


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)
    source_dir = Path(args.source_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / args.jsonl_name

    patientids = load_ipps(Path(args.ipp_file))
    if not patientids:
        LOGGER.warning("No patient IDs provided")
        output_path.write_text("", encoding="utf-8")
        return 0

    hits, docs_seen, unreadable = iter_analysis_hits(source_dir, patientids)
    with output_path.open("w", encoding="utf-8") as handle:
        for item in hits:
            handle.write(json.dumps(asdict(item), ensure_ascii=False) + "\n")

    LOGGER.info(
        "Done | target_ipps=%s | metadata_seen=%s | unreadable=%s | hits=%s | output=%s",
        len(patientids),
        docs_seen,
        unreadable,
        len(hits),
        output_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

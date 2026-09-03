#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import re
import unicodedata
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

try:
    import pymupdf as fitz  # type: ignore
except ImportError:  # pragma: no cover
    try:
        import fitz  # type: ignore
    except ImportError:
        fitz = None

try:
    from PyPDF2 import PdfReader  # type: ignore
except ImportError:  # pragma: no cover
    PdfReader = None


LOGGER = logging.getLogger("osiris_rwd_progression")


@dataclass
class PatientContext:
    patientid: str
    diagnosis_date: Optional[str] = None


@dataclass
class ProgressionHit:
    patientid: str
    progressiondateday: Optional[int]
    progressiondatemonth: Optional[int]
    progressiondateyear: Optional[int]
    progressionsource: str
    source_pdf: str
    source_date: Optional[str]
    matched_text: str
    confidence: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract OSIRIS RWD progression events from PDF reports."
    )
    parser.add_argument("--source-dir", default="/opt/PDF")
    parser.add_argument("--ipp-file", required=True)
    parser.add_argument("--output-dir", default="/opt/extract_osiris_rwd/output")
    parser.add_argument("--jsonl-name", default="progression_results.jsonl")
    parser.add_argument("--progress-every", type=int, default=200)
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


def normalize_text(value: str) -> str:
    value = strip_accents(value)
    value = value.replace("`", "'").replace("’", "'")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def load_context(path: Path) -> dict[str, PatientContext]:
    raw = path.read_text(encoding="utf-8", errors="replace").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {
            token.strip(): PatientContext(patientid=token.strip())
            for token in re.split(r"[,;\r\n]+", raw)
            if token.strip()
        }

    if isinstance(payload, list):
        contexts: dict[str, PatientContext] = {}
        for item in payload:
            if isinstance(item, dict):
                patientid = str(item.get("patientid") or item.get("ipp") or item.get("ipp_ocr") or "").strip()
                diagnosis_date = parse_date(item.get("diagnosis_date"))
            else:
                patientid = str(item).strip()
                diagnosis_date = None
            if patientid:
                contexts[patientid] = PatientContext(patientid=patientid, diagnosis_date=diagnosis_date)
        return contexts

    if isinstance(payload, dict):
        items = payload.get("patients")
        if isinstance(items, list):
            return load_context_from_items(items)
        ipp_list = payload.get("ipp_list") or payload.get("ipps") or []
        return {
            str(value).strip(): PatientContext(patientid=str(value).strip())
            for value in ipp_list
            if str(value).strip()
        }

    raise ValueError("Unsupported IPP file format")


def load_context_from_items(items: list[object]) -> dict[str, PatientContext]:
    contexts: dict[str, PatientContext] = {}
    for item in items:
        if not isinstance(item, dict):
            patientid = str(item).strip()
            if patientid:
                contexts[patientid] = PatientContext(patientid=patientid)
            continue
        patientid = str(item.get("patientid") or item.get("ipp") or item.get("ipp_ocr") or "").strip()
        if patientid:
            contexts[patientid] = PatientContext(
                patientid=patientid,
                diagnosis_date=parse_date(item.get("diagnosis_date")),
            )
    return contexts


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


def metadata_pdf_path(metadata_path: Path) -> Path:
    suffix = ".json.txt"
    if metadata_path.name.lower().endswith(suffix):
        return metadata_path.with_name(metadata_path.name[: -len(suffix)] + ".pdf")
    return metadata_path.with_suffix(".pdf")


def metadata_date(metadata: dict, metadata_path: Path) -> Optional[str]:
    doc = metadata.get("Document") or {}
    episode = metadata.get("Episode") or {}
    for raw in (
        doc.get("CreateDate"),
        doc.get("UpdateDate"),
        doc.get("DocumentDate"),
        episode.get("StartDate"),
    ):
        parsed = parse_date(raw)
        if parsed:
            return parsed

    match = re.search(r"_(\d{8})(?:[_\.]|$)", metadata_path.name)
    if match:
        return parse_date(match.group(1))
    return None


def parse_date(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    token = str(value).strip()
    if not token:
        return None
    digits = re.sub(r"\D", "", token)
    if len(digits) >= 8:
        for fmt in ("%Y%m%d", "%d%m%Y"):
            try:
                return datetime.strptime(digits[:8], fmt).date().isoformat()
            except ValueError:
                pass
    try:
        return date.fromisoformat(token[:10]).isoformat()
    except ValueError:
        return None


def split_date(value: Optional[str]) -> tuple[Optional[int], Optional[int], Optional[int]]:
    parsed = parse_date(value)
    if not parsed:
        return None, None, None
    date_value = date.fromisoformat(parsed)
    return date_value.day, date_value.month, date_value.year


def date_sort_key(value: Optional[str]) -> str:
    return parse_date(value) or "0001-01-01"


def document_fields(metadata: dict) -> tuple[str, str, str]:
    doc = metadata.get("Document") or {}
    return (
        str(doc.get("TypeDescription") or ""),
        str(doc.get("FormatComDesc") or ""),
        str(doc.get("PrescriptionDesc") or ""),
    )


def progression_source_code(metadata: dict, metadata_path: Path) -> str:
    type_desc, format_desc, prescription_desc = document_fields(metadata)
    haystack = normalize_text(" | ".join([type_desc, format_desc, prescription_desc, metadata_path.name])).lower()
    if re.search(r"\b(medecine\s+nucleaire|tep|tepscan|pet\s*scan|scintigraphie)\b", haystack):
        return "4271000179106"
    if re.search(r"\b(scanner|irm|radiologie|echographie|imagerie|radio-senologie|radiographie)\b", haystack):
        return "4201000179104"
    if re.search(r"\b(laboratoire|biologie|resultats?\s+texte|anapath|anatomopathologie)\b", haystack):
        return "4241000179101"
    return "371530004"


def is_progression_source_document(metadata: dict, metadata_path: Path) -> bool:
    type_desc, format_desc, prescription_desc = document_fields(metadata)
    haystack = normalize_text(" | ".join([type_desc, format_desc, prescription_desc, metadata_path.name])).lower()
    if re.search(
        r"\b("
        r"ordonnance|certificat|anesthesie|consentement|bloc|dmi|"
        r"administratif|carte\s+de\s+groupe|cerfa|pharmacie|"
        r"contrat\s+de\s+soins|directives\s+anticipees"
        r")\b",
        haystack,
    ):
        return False
    return bool(
        re.search(r"\b(compte\s+rendu\s+de\s+consultation|consultation|cs\s+|cr\s+consult)\b", haystack)
        or re.search(r"\b(rcp|scanner|irm|tepscan|tep|radiologie|echographie|imagerie|medecine\s+nucleaire)\b", haystack)
        or re.search(r"\b(resultats?\s+texte|hospitalisation|hdj|hco|resume\s+clinique|synthese)\b", haystack)
        or re.search(r"\biuct[.]cr", haystack)
    )


def extract_pdf_text(path: Path) -> str:
    if fitz is not None:
        with fitz.open(path) as doc:
            return "\n".join(page.get_text("text") for page in doc)
    if PdfReader is not None:
        reader = PdfReader(str(path))
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    raise RuntimeError("No PDF backend available: install PyMuPDF or PyPDF2")


def snippet(text: str, start: int, end: int, window: int = 160) -> str:
    left = max(0, start - window)
    right = min(len(text), end + window)
    return re.sub(r"\s+", " ", text[left:right]).strip()


LOCAL_NEGATION = re.compile(r"\b(pas\s+de|sans|absence\s+de|aucun(?:e)?|non)\b", re.IGNORECASE)
RELAPSE_NO = re.compile(
    r"\b(?:absence\s+de\s+rechute|absence\s+de\s+recidive|absence\s+de\s+progression|"
    r"pas\s+d[' ]argument\s+pour\s+progression|pas\s+d[' ]evolutivite|"
    r"reponse\s+complete|remission\s+complete|stabilite\s+sans\s+lesion\s+evolutive)\b",
    re.IGNORECASE,
)
RELAPSE_NO_SURVEILLANCE = re.compile(
    r"\b(?:pas\s+de|absence\s+de|sans|aucun(?:e)?)\s+(?:signe\s+de\s+)?"
    r"(?:rechute|recidive|progression|evolutivite|lesion\s+suspecte|argument\s+pour\s+recidive)\b"
    r"|\b(?:ne\s+retrouv(?:e|ant)\s+pas|pas|aucun(?:e)?|sans)\s+(?:d[' ]?\s*)?"
    r"argument\s+pour\s+(?:une\s+)?(?:rechute|recidive|progression|evolutivite)\b"
    r"|\b(?:persistance\s+de\s+la\s+remission|remission\s+persistante|remission\s+complete|"
    r"remission\s+clinique|controle\s+sans\s+argument\s+pour\s+recidive|pas\s+de\s+lesion\s+suspecte)\b",
    re.IGNORECASE,
)
RELAPSE_CONFIRMED = re.compile(
    r"\b(?:rechute|recidive)\b[\s\S]{0,120}\b(?:confirmee?|documentee?|diagnostiquee?|averee?|"
    r"locale|locoregionale|ganglionnaire|metastatique|tumorale)\b"
    r"|\b(?:reprise\s+evolutive)\b[\s\S]{0,80}\b(?:confirmee?|documentee?|diagnostiquee?|averee?|recidive|rechute)\b"
    r"|\b(?:apparition|nouvelle)\b[\s\S]{0,80}\b(?:lesion\s+tumorale|localisation\s+secondaire|metastase)\b"
    r"|\b(?:metastase|carcinose)\b[\s\S]{0,80}\b(?:confirmee?|documentee?|diagnostiquee?|"
    r"pulmonaire|hepatique|osseuse|cerebrale|pleurale|peritoneale)\b",
    re.IGNORECASE,
)
RELAPSE_AMBIGUOUS_PROGRESSION = re.compile(
    r"\b(?:progression\s+(?:tumorale|locoregionale|metastatique|de\s+la\s+maladie)|"
    r"maladie\s+evolutive|reprise\s+evolutive)\b",
    re.IGNORECASE,
)
RELAPSE_EQUIVOCAL = re.compile(
    r"\b(suspect|suspecte|douteux|douteuse|possible|a\s+controler|a\s+surveiller|"
    r"ne\s+permet\s+pas\s+d[' ]exclure|aspect\s+equivoque|progression\s+non\s+formelle|"
    r"suspicion\s+de\s+progression|suspicion\s+de\s+recidive)\b",
    re.IGNORECASE,
)
RELAPSE_RECAP_CONTEXT = re.compile(
    r"\b(?:histoire\s+de\s+la\s+maladie|antecedent(?:s)?|on\s+rappelle|rappel|diagnostic\s+initial|"
    r"bilan\s+initial|episode\s+initial)\b",
    re.IGNORECASE,
)


def progression_match(text: str) -> Optional[re.Match]:
    normalized = normalize_text(text)
    for pattern in (RELAPSE_CONFIRMED, RELAPSE_AMBIGUOUS_PROGRESSION):
        for match in pattern.finditer(normalized):
            start = max(0, match.start() - 180)
            end = min(len(normalized), match.end() + 180)
            prefix = normalized[start:match.start()]
            window = normalized[start:end]
            if RELAPSE_RECAP_CONTEXT.search(prefix):
                continue
            if LOCAL_NEGATION.search(prefix) or RELAPSE_NO_SURVEILLANCE.search(window) or RELAPSE_NO.search(window):
                continue
            if RELAPSE_EQUIVOCAL.search(window):
                continue
            return match
    return None


def iter_progression_hits(source_dir: Path, contexts: dict[str, PatientContext]) -> tuple[list[ProgressionHit], int, int, int]:
    hits: list[ProgressionHit] = []
    patientids = set(contexts)
    docs_seen = 0
    selected_docs = 0
    read_errors = 0
    seen_keys: set[tuple[str, Optional[str], str]] = set()

    for metadata_path in sorted(source_dir.rglob("*.json.txt")):
        docs_seen += 1
        try:
            metadata = load_metadata(metadata_path)
        except ValueError as exc:
            read_errors += 1
            LOGGER.warning("Unreadable metadata skipped | file=%s | error=%s", metadata_path, exc)
            continue

        ipp = metadata_ipp(metadata, metadata_path)
        if ipp not in patientids or not is_progression_source_document(metadata, metadata_path):
            continue

        pdf_path = metadata_pdf_path(metadata_path)
        if not pdf_path.exists():
            read_errors += 1
            LOGGER.warning("Missing PDF skipped | ipp=%s | metadata=%s", ipp, metadata_path)
            continue

        doc_date = metadata_date(metadata, metadata_path)
        diagnosis_date = contexts[ipp].diagnosis_date
        if diagnosis_date and doc_date and date_sort_key(doc_date) < date_sort_key(diagnosis_date):
            continue

        selected_docs += 1
        try:
            text = extract_pdf_text(pdf_path)
        except Exception as exc:
            read_errors += 1
            LOGGER.warning("Unreadable PDF skipped | ipp=%s | pdf=%s | error=%s", ipp, pdf_path, exc)
            continue

        match = progression_match(text)
        if not match:
            continue

        key = (ipp, doc_date, pdf_path.name)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        day, month, year = split_date(doc_date)
        normalized_text = normalize_text(text)
        hits.append(
            ProgressionHit(
                patientid=ipp,
                progressiondateday=day,
                progressiondatemonth=month,
                progressiondateyear=year,
                progressionsource=progression_source_code(metadata, metadata_path),
                source_pdf=pdf_path.name,
                source_date=doc_date,
                matched_text=snippet(normalized_text, match.start(), match.end()),
                confidence="regex",
            )
        )

    return hits, docs_seen, selected_docs, read_errors


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)
    source_dir = Path(args.source_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / args.jsonl_name

    if fitz is None and PdfReader is None:
        raise RuntimeError("No PDF backend available: install PyMuPDF or PyPDF2")

    contexts = load_context(Path(args.ipp_file))
    if not contexts:
        LOGGER.warning("No patient IDs provided")
        output_path.write_text("", encoding="utf-8")
        return 0

    hits, docs_seen, selected_docs, read_errors = iter_progression_hits(source_dir, contexts)
    with output_path.open("w", encoding="utf-8") as handle:
        for item in hits:
            handle.write(json.dumps(asdict(item), ensure_ascii=False) + "\n")

    LOGGER.info(
        "Done | target_ipps=%s | docs_seen=%s | selected_docs=%s | read_errors=%s | hits=%s | output=%s",
        len(contexts),
        docs_seen,
        selected_docs,
        read_errors,
        len(hits),
        output_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

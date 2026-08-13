#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import unicodedata
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

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


LOGGER = logging.getLogger("osiris_rwd_riskfactor")

RISK_TOBACCO = "Exposition au tabac"
RISK_ALCOHOL = "Consommation d'alcool"
RISK_OVERWEIGHT = "Surpoids et/ou obésité"
RISK_HORMONES = "Utilisation d'hormones exogènes"
RISK_INFECTION = "Agents infectieux oncogènes"
RISK_RADIATION = "Radiations ionisantes"
RISK_UV = "Rayonnement solaire (UV)"
RISK_OCCUPATIONAL = "Expositions professionnelles"
RISK_CHEMICAL = "Exposition à certaines substances chimiques"


@dataclass
class RiskFactorHit:
    patientid: str
    riskfactortype: str
    riskfactorvalue: Optional[bool]
    pathogen: Optional[str]
    source_pdf: str
    source_date: Optional[str]
    matched_text: str
    confidence: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract OSIRIS RWD risk factors from consultation PDF reports."
    )
    parser.add_argument("--source-dir", default="/opt/PDF")
    parser.add_argument("--ipp-file", required=True)
    parser.add_argument("--output-dir", default="/opt/extract_osiris_rwd/output")
    parser.add_argument("--jsonl-name", default="riskfactor_results.jsonl")
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
    value = value.replace("’", "'").replace("`", "'")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_lines(value: str) -> str:
    value = strip_accents(value)
    value = value.replace("â€™", "'").replace("`", "'").replace("\r", "\n")
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in value.splitlines()]
    return "\n".join(lines)


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


def metadata_pdf_path(metadata_path: Path) -> Path:
    suffix = ".json.txt"
    if metadata_path.name.lower().endswith(suffix):
        return metadata_path.with_name(metadata_path.name[: -len(suffix)] + ".pdf")
    return metadata_path.with_suffix(".pdf")


def metadata_date(metadata: dict) -> Optional[str]:
    doc = metadata.get("Document") or {}
    episode = metadata.get("Episode") or {}
    for raw in (
        doc.get("CreateDate"),
        doc.get("UpdateDate"),
        episode.get("StartDate"),
    ):
        token = str(raw or "").strip()[:8]
        if len(token) == 8 and token.isdigit():
            try:
                return datetime.strptime(token, "%Y%m%d").date().isoformat()
            except ValueError:
                continue
    return None


def document_fields(metadata: dict) -> tuple[str, str, str]:
    doc = metadata.get("Document") or {}
    return (
        str(doc.get("TypeDescription") or ""),
        str(doc.get("FormatComDesc") or ""),
        str(doc.get("PrescriptionDesc") or ""),
    )


def is_consultation_document(metadata: dict) -> bool:
    type_desc, format_desc, prescription_desc = document_fields(metadata)
    haystack = normalize_text(" | ".join([type_desc, format_desc, prescription_desc])).lower()
    if re.search(
        r"\b("
        r"anapath|anatomopath|histolog|cytolog|"
        r"ordonnance|certificat|anesthesie|consentement|"
        r"operatoire|bloc|dmi|scanner|irm|tepscan|tep|imagerie|"
        r"radiologie|echographie|medecine\s+nucleaire|rcp|"
        r"lettre\s+de\s+liaison|hospitalisation|hdj|hco"
        r")\b",
        haystack,
    ):
        return False
    return bool(
        re.search(r"\bcompte\s+rendu\s+de\s+consultation\b", haystack)
        or re.search(r"\bcr\s+ou\s+fiche\s+de\s+consultation\s+ou\s+de\s+visite\b", haystack)
        or re.search(r"\bcs\s+", haystack)
        or re.search(r"\biuct[.]crcssur\b", haystack)
        or re.search(r"\biuct[.]crcsnv\b", haystack)
        or re.search(r"\bconsultation\b", haystack)
        or re.search(r"\bcr\s+consult", haystack)
        or re.search(r"\bcompte\s+rendu\s+de\s+consult", haystack)
    )


def extract_pdf_text(path: Path) -> str:
    if fitz is not None:
        with fitz.open(path) as doc:
            return "\n".join(page.get_text("text") for page in doc)
    if PdfReader is not None:
        reader = PdfReader(str(path))
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    raise RuntimeError("No PDF backend available: install PyMuPDF or PyPDF2")


def snippet(text: str, start: int, end: int, window: int = 120) -> str:
    left = max(0, start - window)
    right = min(len(text), end + window)
    return re.sub(r"\s+", " ", text[left:right]).strip()


def hit(
    patientid: str,
    riskfactortype: str,
    value: Optional[bool],
    pathogen: Optional[str],
    pdf: Path,
    source_date: Optional[str],
    matched_text: str,
    confidence: str = "regex",
) -> RiskFactorHit:
    return RiskFactorHit(
        patientid=patientid,
        riskfactortype=riskfactortype,
        riskfactorvalue=value,
        pathogen=pathogen,
        source_pdf=pdf.name,
        source_date=source_date,
        matched_text=matched_text[:500],
        confidence=confidence,
    )


def first_match(patterns: Iterable[re.Pattern], text: str) -> Optional[re.Match]:
    matches = [match for pattern in patterns for match in pattern.finditer(text)]
    if not matches:
        return None
    return min(matches, key=lambda match: match.start())


RISK_SECTION_START = re.compile(
    r"(?im)^\s*(ant[ée]c[ée]dents?|comorbidit[ée]s?|facteurs?\s+de\s+risques?|mode\s+de\s+vie)\s*[:：]?\s*$"
)
RISK_SECTION_STOP = re.compile(
    r"(?im)^\s*("
    r"ant[ée]c[ée]dents?\s+(?:oncologiques?\s+)?familiaux|"
    r"ant[ée]c[ée]dents?\s+familiaux|"
    r"familiaux|histoire\s+familiale|"
    r"allergies?|traitements?\s+en\s+cours|examen\s+clinique|"
    r"histoire\s+de\s+la\s+maladie|conclusion|synth[èe]se|prise\s+en\s+charge"
    r")\s*[:：]?\s*$"
)


# Text is accent-stripped before these regexes run. These stricter patterns keep
# matches inside clinical sections and handle headings with content on same line.
RISK_SECTION_START = re.compile(
    r"(?im)^[ \t]*("
    r"antecedents?(?![^\n]{0,80}famil)"
    r"(?:[ \t]+(?:medicaux|chirurgicaux|personnels|gynecologiques|obstetricaux|notables))*|"
    r"comorbidites?|"
    r"facteurs?[ \t]+de[ \t]+risques?|"
    r"mode[ \t]+de[ \t]+vie"
    r")[ \t]*[:：]?[ \t]*"
)
RISK_SECTION_STOP = re.compile(
    r"(?im)^[ \t]*("
    r"antecedents?[ \t]+(?:oncologiques?[ \t]+)?familiaux|"
    r"familiaux|histoire[ \t]+familiale|"
    r"allergies?|traitements?[ \t]+en[ \t]+cours|examen[ \t]+clinique|"
    r"histoire[ \t]+de[ \t]+la[ \t]+maladie|conclusion|synthese|prise[ \t]+en[ \t]+charge"
    r")[ \t]*[:：]?[ \t]*"
)
REPORT_BODY_MARKER = re.compile(
    r"(?im)^[ \t]*("
    r"compte[ -]?rendu|"
    r"chere?[ \t]+consoeur|cher[ \t]+confrere|"
    r"j[' ]ai[ \t]+vu|"
    r"motif[ \t]+de[ \t]+venue|"
    r"antecedents?|comorbidites?|facteurs?[ \t]+de[ \t]+risques?|mode[ \t]+de[ \t]+vie"
    r")\b"
)


def clinical_body_text(raw_text: str) -> str:
    normalized = normalize_lines(raw_text)
    marker = REPORT_BODY_MARKER.search(normalized)
    if not marker:
        return normalized
    return normalized[marker.start() :]


def riskfactor_scope(raw_text: str) -> str:
    normalized = clinical_body_text(raw_text)
    starts = list(RISK_SECTION_START.finditer(normalized))
    if not starts:
        return ""

    chunks: list[str] = []
    for index, start_match in enumerate(starts):
        start = start_match.end()
        next_start = starts[index + 1].start() if index + 1 < len(starts) else len(normalized)
        stop_match = RISK_SECTION_STOP.search(normalized, start, next_start)
        end = stop_match.start() if stop_match else next_start
        chunk = normalized[start:end].strip()
        if chunk:
            chunks.append(chunk)

    return "\n".join(chunks)


TOBACCO_POSITIVE = [
    re.compile(r"\b(ancien(?:ne)?\s+fumeur|ex[- ]?fumeur|tabagisme\s+(?:actif|sevre|ancien)|fumeur|fumeuse|paquet[s]?[- ]?annee[s]?)\b", re.I),
]
TOBACCO_NEGATIVE = [
    re.compile(r"\b(pas\s+de\s+tabagisme|non\s+fumeur|non\s+fumeuse|absence\s+de\s+tabagisme|n'a\s+jamais\s+fume)\b", re.I),
]
ALCOHOL_POSITIVE = [
    re.compile(r"\b(alcoolisme|ethylisme|consommation\s+alcoolique|alcool\s+chronique|sevrage\s+alcool)\b", re.I),
]
ALCOHOL_NEGATIVE = [
    re.compile(r"\b(pas\s+d[' ]alcool|absence\s+de\s+consommation\s+alcool|non\s+alcoolique|alcool\s*:\s*(?:non|0))\b", re.I),
]
OVERWEIGHT_POSITIVE = [
    re.compile(r"\b(obesite|obese|surpoids|surcharge\s+ponderale)\b", re.I),
]
OVERWEIGHT_NEGATIVE = [
    re.compile(r"\b(pas\s+d[' ]obesite|absence\s+d[' ]obesite|imc\s+normal|bmi\s+normal)\b", re.I),
]
BMI_PATTERN = re.compile(
    r"\b(?:poids\s*(?:=|:)?\s*)?(\d{2,3}(?:[,.]\d+)?)\s*kg\b.{0,80}?\b(?:taille\s*(?:=|:)?\s*)?(1[,.]\d{2}|[12]\d{2})\s*m?\b",
    re.I,
)
HORMONES_POSITIVE = [
    re.compile(r"\b(contraception\s+hormonale|pilule|traitement\s+hormonal|hormonotherapie\s+substitutive|ths|estrogene|progestatif)\b", re.I),
]
HORMONES_NEGATIVE = [
    re.compile(r"\b(pas\s+de\s+contraception\s+hormonale|absence\s+de\s+traitement\s+hormonal|pas\s+de\s+ths)\b", re.I),
]
RADIATION_POSITIVE = [
    re.compile(r"\b(radiotherapie\s+(?:dans\s+l[' ]enfance|anterieure|pour\s+lymphome|pour\s+angiome)|irradiation\s+anterieure|exposition\s+au\s+radon)\b", re.I),
]
UV_POSITIVE = [
    re.compile(r"\b(exposition\s+solaire|rayonnement\s+solaire|uv|cabine\s+uv|ultraviolet)\b", re.I),
]
UV_NEGATIVE = [
    re.compile(r"\b(pas\s+d[' ]exposition\s+solaire|absence\s+d[' ]exposition\s+solaire|pas\s+d[' ]uv)\b", re.I),
]
OCCUPATIONAL_POSITIVE = [
    re.compile(r"\b(exposition\s+professionnelle|amiante|asbestos|poussieres?|solvants?|pesticides?)\b", re.I),
]
CHEMICAL_POSITIVE = [
    re.compile(r"\b(arsenic|benzene|hydrocarbures?|produits?\s+chimiques?|exposition\s+chimique|solvants?|pesticides?)\b", re.I),
]
PATHOGENS = [
    ("Clonorchis sinensis", re.compile(r"\bclonorchis\s+sinensis\b", re.I)),
    ("Helicobacter pylori", re.compile(r"\b(h[.]?\s*pylori|helicobacter\s+pylori)\b", re.I)),
    ("Opisthorchis viverrini", re.compile(r"\bopisthorchis\s+viverrini\b", re.I)),
    ("Schistosoma haematobium", re.compile(r"\bschistosoma\s+haematobium\b", re.I)),
    ("Hepatitis B", re.compile(r"\b(hepatite\s+b|vhb|hbv)\b", re.I)),
    ("Hepatitis C", re.compile(r"\b(hepatite\s+c|vhc|hcv)\b", re.I)),
    ("HIV type 1", re.compile(r"\b(vih|hiv)\b", re.I)),
    ("Human herpesvirus 4", re.compile(r"\b(ebv|epstein[- ]barr|human\s+herpesvirus\s+4)\b", re.I)),
    ("Human T-cell lymphotropic type 1", re.compile(r"\b(htlv|human\s+t[- ]cell\s+lymphotropic)\b", re.I)),
    ("Human papillomavirus", re.compile(r"\b(hpv|papillomavirus)\b", re.I)),
    ("HHV-8", re.compile(r"\b(hhv[- ]?8|herpesvirus\s+humain\s+8)\b", re.I)),
]
PATHOGEN_NEGATION_PATTERN = re.compile(
    r"\b("
    r"negatif|negative|negatifs|negatives|"
    r"serologies?\s+[^.;:\n]{0,120}\s+negatives?|"
    r"absence\s+de|sans|pas\s+de|non\s+retrouve|non\s+detecte"
    r")\b",
    re.I,
)


def is_negated_pathogen_context(text: str, start: int, end: int) -> bool:
    left = max(0, start - 120)
    right = min(len(text), end + 120)
    context = text[left:right]
    return bool(PATHOGEN_NEGATION_PATTERN.search(context))


def extract_hits_for_document(
    patientid: str,
    pdf: Path,
    source_date: Optional[str],
    raw_text: str,
    include_negated_pathogens: bool = False,
) -> list[RiskFactorHit]:
    text = normalize_text(riskfactor_scope(raw_text))
    hits: list[RiskFactorHit] = []

    checks = [
        (RISK_TOBACCO, TOBACCO_NEGATIVE, False),
        (RISK_TOBACCO, TOBACCO_POSITIVE, True),
        (RISK_ALCOHOL, ALCOHOL_NEGATIVE, False),
        (RISK_ALCOHOL, ALCOHOL_POSITIVE, True),
        (RISK_OVERWEIGHT, OVERWEIGHT_NEGATIVE, False),
        (RISK_OVERWEIGHT, OVERWEIGHT_POSITIVE, True),
        (RISK_HORMONES, HORMONES_NEGATIVE, False),
        (RISK_HORMONES, HORMONES_POSITIVE, True),
        (RISK_RADIATION, RADIATION_POSITIVE, True),
        (RISK_UV, UV_NEGATIVE, False),
        (RISK_UV, UV_POSITIVE, True),
        (RISK_OCCUPATIONAL, OCCUPATIONAL_POSITIVE, True),
        (RISK_CHEMICAL, CHEMICAL_POSITIVE, True),
    ]

    for risk_type, patterns, value in checks:
        match = first_match(patterns, text)
        if match:
            hits.append(hit(patientid, risk_type, value, None, pdf, source_date, snippet(text, match.start(), match.end())))

    bmi_match = BMI_PATTERN.search(text)
    if bmi_match:
        weight = float(bmi_match.group(1).replace(",", "."))
        height_raw = bmi_match.group(2).replace(",", ".")
        height = float(height_raw)
        if height > 10:
            height = height / 100
        if height > 0:
            bmi = weight / (height * height)
            hits.append(
                hit(
                    patientid,
                    RISK_OVERWEIGHT,
                    bmi >= 25,
                    None,
                    pdf,
                    source_date,
                    f"{snippet(text, bmi_match.start(), bmi_match.end())} | BMI={bmi:.2f}",
                    "regex_bmi",
                )
            )

    for pathogen, pattern in PATHOGENS:
        match = pattern.search(text)
        if match:
            value = not is_negated_pathogen_context(text, match.start(), match.end())
            if not value and not include_negated_pathogens:
                continue
            hits.append(
                hit(
                    patientid,
                    RISK_INFECTION,
                    value,
                    pathogen,
                    pdf,
                    source_date,
                    snippet(text, match.start(), match.end()),
                    "regex_negated" if not value else "regex",
                )
            )

    return hits


def choose_best_hits(hits: list[RiskFactorHit]) -> list[RiskFactorHit]:
    best: dict[tuple[str, str, Optional[str]], RiskFactorHit] = {}
    rank = {True: 2, False: 1, None: 0}
    for current in hits:
        key = (current.patientid, current.riskfactortype, current.pathogen)
        previous = best.get(key)
        if previous is None or rank[current.riskfactorvalue] > rank[previous.riskfactorvalue]:
            best[key] = current
    return list(best.values())


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)

    source_dir = Path(args.source_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / args.jsonl_name

    target_ipps = load_ipps(Path(args.ipp_file))
    if not target_ipps:
        LOGGER.warning("No IPP received; writing empty output.")
        output_path.write_text("", encoding="utf-8")
        return 0

    metadata_files = sorted(source_dir.glob("*.json.txt"))
    LOGGER.info("Scanning %s metadata files in %s for %s IPP", len(metadata_files), source_dir, len(target_ipps))

    all_hits: list[RiskFactorHit] = []
    docs_seen = 0
    docs_selected = 0
    docs_read_error = 0

    for index, metadata_path in enumerate(metadata_files, start=1):
        if args.progress_every and index % args.progress_every == 0:
            LOGGER.info("Progress metadata %s/%s | hits=%s", index, len(metadata_files), len(all_hits))
        try:
            metadata = load_metadata(metadata_path)
        except Exception:
            continue
        patientid = metadata_ipp(metadata, metadata_path)
        if patientid not in target_ipps:
            continue
        docs_seen += 1
        if not is_consultation_document(metadata):
            continue
        pdf_path = metadata_pdf_path(metadata_path)
        if not pdf_path.exists():
            continue
        docs_selected += 1
        try:
            text = extract_pdf_text(pdf_path)
        except Exception as exc:
            docs_read_error += 1
            LOGGER.warning("Unreadable PDF skipped | ipp=%s | pdf=%s | error=%s", patientid, pdf_path, exc)
            continue
        all_hits.extend(extract_hits_for_document(patientid, pdf_path, metadata_date(metadata), text))

    selected_hits = choose_best_hits(all_hits)
    with output_path.open("w", encoding="utf-8") as handle:
        for row in selected_hits:
            handle.write(json.dumps(asdict(row), ensure_ascii=False) + "\n")

    LOGGER.info(
        "Done | target_ipps=%s | docs_seen=%s | consultation_docs=%s | read_errors=%s | hits=%s | output=%s",
        len(target_ipps),
        docs_seen,
        docs_selected,
        docs_read_error,
        len(selected_hits),
        output_path,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

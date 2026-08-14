#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

from extract_riskfactor_by_ipp import (
    configure_logging,
    document_fields,
    extract_pdf_text,
    is_consultation_document,
    load_ipps,
    load_metadata,
    metadata_date,
    metadata_ipp,
    metadata_pdf_path,
    normalize_lines,
    normalize_text,
    snippet,
)


LOGGER = logging.getLogger("osiris_rwd_familycancerhistory")


@dataclass
class FamilyCancerHit:
    patientid: str
    familycancertopocode: str
    familycancerparentage: str
    source_pdf: str
    source_date: Optional[str]
    matched_text: str
    confidence: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract OSIRIS RWD family cancer history from consultation PDF reports."
    )
    parser.add_argument("--source-dir", default="/opt/PDF")
    parser.add_argument("--ipp-file", required=True)
    parser.add_argument("--output-dir", default="/opt/extract_osiris_rwd/output")
    parser.add_argument("--jsonl-name", default="familycancerhistory_results.jsonl")
    parser.add_argument("--progress-every", type=int, default=200)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def is_family_source_document(metadata: dict) -> bool:
    if is_consultation_document(metadata):
        return True

    type_desc, format_desc, prescription_desc = document_fields(metadata)
    haystack = normalize_text(" | ".join([type_desc, format_desc, prescription_desc])).lower()
    if re.search(
        r"\b("
        r"anapath|anatomopath|histolog|cytolog|ordonnance|certificat|"
        r"anesthesie|consentement|operatoire|bloc|dmi|tepscan|tep|"
        r"medecine\s+nucleaire|rcp|scanner|irm"
        r")\b",
        haystack,
    ):
        return False

    return bool(
        re.search(r"\b(compte\s+rendu\s+radio\s+senologie|radio\s+senologie|senologie)\b", haystack)
        or re.search(r"\b(mammographie|echographie\s+mammaire)\b", haystack)
    )


FAMILY_SECTION_START = re.compile(
    r"(?im)^[ \t]*("
    r"antecedents?[ \t]+(?:carcinologiques?|oncologiques?)?[ \t]*familiaux|"
    r"antecedents?[ \t]+familiaux|"
    r"histoire[ \t]+familiale|"
    r"antecedents?[ \t]+senologiques?"
    r")[ \t]*[:：]?[ \t]*"
)
FAMILY_SECTION_STOP = re.compile(
    r"(?im)^[ \t]*("
    r"traitements?[ \t]+(?:habituel|en[ \t]+cours|quotidien)|"
    r"allergies?|histoire[ \t]+de[ \t]+la[ \t]+maladie|"
    r"examen[ \t]+clinique|conclusion|synthese|prise[ \t]+en[ \t]+charge|"
    r"antecedents?[ \t]+(?:medicaux|chirurgicaux|personnels|gynecologiques|obstetricaux)|"
    r"comorbidites?|facteurs?[ \t]+de[ \t]+risques?"
    r")[ \t]*[:：]?[ \t]*"
)
BODY_MARKER = re.compile(
    r"(?im)^[ \t]*("
    r"compte[ -]?rendu|chere?[ \t]+consoeur|cher[ \t]+confrere|"
    r"j[' ]ai[ \t]+vu|motif[ \t]+de[ \t]+venue|"
    r"antecedents?|antecedent|histoire[ \t]+familiale"
    r")\b"
)


PARENTAGE_PATTERNS = [
    ("Mother", re.compile(r"\b(mere|maman)\b", re.I)),
    ("Father", re.compile(r"\b(pere|papa)\b", re.I)),
    ("Sibling", re.compile(r"\b(frere|soeur)\b", re.I)),
    ("Children", re.compile(r"\b(fille|fils|enfant)\b", re.I)),
    ("Grandparent", re.compile(r"\b(grand[- ]mere|grand[- ]pere|grands[- ]parents?)\b", re.I)),
    ("Aunt/Uncle", re.compile(r"\b(oncle|tante)\b", re.I)),
    ("Cousin", re.compile(r"\b(cousin|cousine)\b", re.I)),
]

TOPO_PATTERNS = [
    ("C50", re.compile(r"\b(sein|mammaire|senologique)\b", re.I)),
    ("C56", re.compile(r"\b(ovaire|ovarien|ovarienne)\b", re.I)),
    ("C18", re.compile(r"\b(colon|colique|colorectal)\b", re.I)),
    ("C20", re.compile(r"\b(rectum|rectal)\b", re.I)),
    ("C61", re.compile(r"\b(prostate|prostatique)\b", re.I)),
    ("C25", re.compile(r"\b(pancreas|pancreatique)\b", re.I)),
    ("C34", re.compile(r"\b(poumon|pulmonaire|bronchique)\b", re.I)),
    ("C54", re.compile(r"\b(endometre|uterus|uterin)\b", re.I)),
    ("C53", re.compile(r"\b(col[ \t]+de[ \t]+l[' ]uterus|col[ \t]+uterin)\b", re.I)),
    ("C43", re.compile(r"\b(melanome)\b", re.I)),
    ("C44", re.compile(r"\b(carcinome[ \t]+basocellulaire|carcinome[ \t]+epidermoide[ \t]+cutane|peau)\b", re.I)),
    ("C81-C85", re.compile(r"\b(lymphome|hodgkin)\b", re.I)),
    ("C91-C95", re.compile(r"\b(leucemie)\b", re.I)),
    ("C71", re.compile(r"\b(cerveau|cerebral|encephale|tronc[ \t]+cerebral|glioblastome)\b", re.I)),
    ("C16", re.compile(r"\b(estomac|gastrique)\b", re.I)),
    ("C22", re.compile(r"\b(foie|hepatique)\b", re.I)),
    ("C64", re.compile(r"\b(rein|renal)\b", re.I)),
    ("C67", re.compile(r"\b(vessie|vesical)\b", re.I)),
    ("C73", re.compile(r"\b(thyroide|thyroidien)\b", re.I)),
]

FAMILY_NEGATION = re.compile(
    r"\b("
    r"pas[ \t]+d[' ]antecedents?[ \t]+familiaux|"
    r"aucun(?:e)?[ \t]+antecedents?[ \t]+familiaux|"
    r"absence[ \t]+d[' ]antecedents?[ \t]+familiaux|"
    r"pas[ \t]+d[' ]histoire[ \t]+familiale"
    r")\b",
    re.I,
)
CANCER_WORD = re.compile(r"\b(cancer|neoplasie|tumeur[ \t]+maligne|carcinome|melanome|lymphome|leucemie)\b", re.I)


def clinical_body_text(raw_text: str) -> str:
    normalized = normalize_lines(raw_text)
    marker = BODY_MARKER.search(normalized)
    if not marker:
        return normalized
    return normalized[marker.start() :]


def family_scope(raw_text: str) -> str:
    normalized = clinical_body_text(raw_text)
    starts = list(FAMILY_SECTION_START.finditer(normalized))
    if not starts:
        return ""

    chunks: list[str] = []
    for index, start_match in enumerate(starts):
        start = start_match.start()
        next_start = starts[index + 1].start() if index + 1 < len(starts) else len(normalized)
        stop_match = FAMILY_SECTION_STOP.search(normalized, start_match.end(), next_start)
        end = stop_match.start() if stop_match else next_start
        chunk = normalized[start:end].strip()
        if chunk and not FAMILY_NEGATION.search(chunk):
            chunks.append(chunk)
    return "\n".join(chunks)


def split_candidates(text: str) -> list[str]:
    return [
        part.strip(" -\t")
        for part in re.split(r"(?:[.;]\s+|\n+|(?:\s+-\s+))", text)
        if part.strip(" -\t")
    ]


def parentages(sentence: str) -> list[str]:
    return [label for label, pattern in PARENTAGE_PATTERNS if pattern.search(sentence)]


def topo_codes(sentence: str, section_text: str) -> list[str]:
    found = [code for code, pattern in TOPO_PATTERNS if pattern.search(sentence)]
    if found:
        return found
    if re.search(r"\b(antecedents?[ \t]+senologiques?|senologique)\b", section_text, re.I) and re.search(
        r"\b(diagnostiquee?|diagnostique|decedee?|traitee?)\b", sentence, re.I
    ):
        return ["C50"]
    return []


def extract_hits_for_document(
    patientid: str,
    pdf: Path,
    source_date: Optional[str],
    raw_text: str,
) -> list[FamilyCancerHit]:
    section = normalize_text(family_scope(raw_text))
    if not section:
        return []

    hits: list[FamilyCancerHit] = []
    for sentence in split_candidates(section):
        if FAMILY_NEGATION.search(sentence):
            continue
        parents = parentages(sentence)
        if not parents:
            continue
        codes = topo_codes(sentence, section)
        if not codes:
            continue
        if not CANCER_WORD.search(sentence) and "C50" not in codes:
            continue
        for parentage in parents:
            for topo in codes:
                hits.append(
                    FamilyCancerHit(
                        patientid=patientid,
                        familycancertopocode=topo,
                        familycancerparentage=parentage,
                        source_pdf=pdf.name,
                        source_date=source_date,
                        matched_text=sentence[:500],
                        confidence="regex",
                    )
                )
    return hits


def choose_best_hits(hits: list[FamilyCancerHit]) -> list[FamilyCancerHit]:
    best: dict[tuple[str, str, str], FamilyCancerHit] = {}
    for current in hits:
        key = (current.patientid, current.familycancertopocode, current.familycancerparentage)
        best.setdefault(key, current)
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

    all_hits: list[FamilyCancerHit] = []
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
        if not is_family_source_document(metadata):
            continue
        docs_selected += 1
        pdf_path = metadata_pdf_path(metadata_path)
        if not pdf_path.exists():
            continue
        try:
            raw_text = extract_pdf_text(pdf_path)
        except Exception as exc:
            docs_read_error += 1
            LOGGER.warning("Unreadable PDF skipped | ipp=%s | pdf=%s | error=%s", patientid, pdf_path, exc)
            continue
        all_hits.extend(extract_hits_for_document(patientid, pdf_path, metadata_date(metadata), raw_text))

    selected_hits = choose_best_hits(all_hits)
    with output_path.open("w", encoding="utf-8") as handle:
        for current in selected_hits:
            handle.write(json.dumps(asdict(current), ensure_ascii=False) + "\n")

    LOGGER.info(
        "Done | target_ipps=%s | docs_seen=%s | family_docs=%s | read_errors=%s | hits=%s | output=%s",
        len(target_ipps),
        docs_seen,
        docs_selected,
        docs_read_error,
        len(selected_hits),
        output_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

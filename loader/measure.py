from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


FILE_PATH = "/tmp/etl_iris/observations.jsonl"
BATCH_SIZE = 5000
PROGRESS_EVERY = 100000


def parse_date(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    value = str(value).strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def split_date(value):
    parsed = parse_date(value)
    if not parsed:
        return None, None, None
    return parsed.day, parsed.month, parsed.year


def clean_text(value):
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def parse_number(value):
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value))
    text = str(value).strip().replace(",", ".")
    if not text:
        return None
    match = re.search(r"-?\d+(?:[.]\d+)?", text)
    if not match:
        return None
    try:
        return Decimal(match.group(0))
    except InvalidOperation:
        return None


def infer_unit(label):
    if re.search(r"\b(poids|weight)\b", label):
        return "kg"
    if re.search(r"\b(taille|height)\b", label):
        return "m"
    if re.search(r"\b(imc|bmi|indice de masse corporelle)\b", label):
        return "kg/m2"
    if re.search(r"\b(temp|temperature)\b", label):
        return "C"
    if re.search(r"\b(saturation|spo2|sao2)\b", label):
        return "%"
    if re.search(r"\b(pouls|frequence cardiaque|fc)\b", label):
        return "/min"
    if re.search(r"\b(tension|pression arterielle|pa)\b", label):
        return "mmHg"
    return None


def normalize_measure(row):
    label = " ".join(
        clean_text(row.get(key)) or ""
        for key in ("item_code", "item_libelle", "type_observation", "valeur_libelle")
    ).lower()
    measuretype = (
        clean_text(row.get("item_libelle"))
        or clean_text(row.get("item_code"))
        or clean_text(row.get("type_observation"))
    )
    raw_value = row.get("valeur_numerique")
    if raw_value is None:
        raw_value = row.get("valeur_brute")
    value = parse_number(raw_value)
    if value is None or not measuretype:
        return None

    if re.search(r"\b(poids|weight)\b", label):
        return "Weight", value, "kg"
    if re.search(r"\b(taille|height)\b", label):
        if value > 3:
            value = value / Decimal("100")
        return "Height", value, "m"
    if re.search(r"\b(imc|bmi|indice de masse corporelle)\b", label):
        return "BMI", value, "kg/m2"

    return measuretype, value, infer_unit(label)


def get_loaded_patientids(cur):
    cur.execute("SELECT patientid FROM osiris_rwd.patient")
    return {str(row[0]).strip() for row in cur.fetchall() if row[0]}


def count_file_lines(path):
    total = 0
    with open(path, "rb") as handle:
        for _ in handle:
            total += 1
    return total


def load_measure():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        patientids = get_loaded_patientids(cur)
        print(f"Measure load started: patientids={len(patientids)}; source={FILE_PATH}")
        cur.execute("TRUNCATE TABLE osiris_rwd.measure RESTART IDENTITY")
        print("Measure target truncated")

        total_lines = count_file_lines(FILE_PATH)
        print(f"Measure source lines: {total_lines}")

        insert_sql = """
            INSERT INTO osiris_rwd.measure (
                patientid,
                measuretype,
                measurevalue,
                measureunit,
                measuredateday,
                measuredatemonth,
                measuredateyear
            )
            VALUES %s
        """

        inserted = 0
        processed = 0
        skipped_not_patient = 0
        skipped_no_numeric_value = 0
        skipped_no_date = 0
        seen = set()
        batch = []
        with open(FILE_PATH, "r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                processed += 1
                row = json.loads(line)
                patientid = clean_text(row.get("ipp") or row.get("ipp_ocr"))
                if not patientid or patientid not in patientids:
                    skipped_not_patient += 1
                    continue

                normalized = normalize_measure(row)
                if not normalized:
                    skipped_no_numeric_value += 1
                    continue
                measuretype, measurevalue, measureunit = normalized

                measure_date = parse_date(row.get("date_observation") or row.get("date_admission"))
                day, month, year = split_date(measure_date)
                if not month or not year:
                    skipped_no_date += 1
                    continue

                key = (patientid, measuretype, str(measurevalue), measureunit, day, month, year)
                if key in seen:
                    continue
                seen.add(key)

                batch.append(
                    (
                        patientid,
                        measuretype,
                        measurevalue,
                        measureunit,
                        day,
                        month,
                        year,
                    ),
                )
                if len(batch) >= BATCH_SIZE:
                    execute_values(cur, insert_sql, batch, page_size=BATCH_SIZE)
                    inserted += len(batch)
                    batch.clear()

                if processed % PROGRESS_EVERY == 0:
                    print(
                        "Measure progress: "
                        f"processed={processed}/{total_lines}; inserted={inserted}; "
                        f"skipped_not_patient={skipped_not_patient}; "
                        f"skipped_no_numeric_value={skipped_no_numeric_value}; "
                        f"skipped_no_date={skipped_no_date}"
                    )

        if batch:
            execute_values(cur, insert_sql, batch, page_size=BATCH_SIZE)
            inserted += len(batch)
            print(f"Measure final batch inserted; inserted={inserted}")

        conn.commit()
        print(
            "Measure rows inserted: "
            f"{inserted}; rows processed: {processed}; skipped_not_patient: {skipped_not_patient}; "
            f"skipped_no_numeric_value: {skipped_no_numeric_value}; skipped_no_date: {skipped_no_date}"
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

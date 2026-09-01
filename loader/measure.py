from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from airflow.providers.postgres.hooks.postgres import PostgresHook


FILE_PATH = "/tmp/etl_iris/observations.jsonl"


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


def normalize_measure(row):
    label = " ".join(
        clean_text(row.get(key)) or ""
        for key in ("item_code", "item_libelle", "type_observation", "valeur_libelle")
    ).lower()
    raw_value = row.get("valeur_numerique")
    if raw_value is None:
        raw_value = row.get("valeur_brute")
    value = parse_number(raw_value)
    if value is None:
        return None

    if re.search(r"\b(poids|weight)\b", label):
        return "Weight", value, "kg"
    if re.search(r"\b(taille|height)\b", label):
        if value > 3:
            value = value / Decimal("100")
        return "Height", value, "m"
    if re.search(r"\b(imc|bmi|indice de masse corporelle)\b", label):
        return "BMI", value, "kg/m2"

    return None


def get_loaded_patientids(cur):
    cur.execute("SELECT patientid FROM osiris_rwd.patient")
    return {str(row[0]).strip() for row in cur.fetchall() if row[0]}


def load_measure():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        patientids = get_loaded_patientids(cur)
        cur.execute("TRUNCATE TABLE osiris_rwd.measure RESTART IDENTITY")

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
            SELECT %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1
                FROM osiris_rwd.measure
                WHERE patientid = %s
                  AND measuretype = %s
                  AND measurevalue IS NOT DISTINCT FROM %s
                  AND measureunit IS NOT DISTINCT FROM %s
                  AND measuredateday IS NOT DISTINCT FROM %s
                  AND measuredatemonth IS NOT DISTINCT FROM %s
                  AND measuredateyear IS NOT DISTINCT FROM %s
            )
        """

        inserted = 0
        seen = set()
        with open(FILE_PATH, "r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)
                patientid = clean_text(row.get("ipp"))
                if not patientid or patientid not in patientids:
                    continue

                normalized = normalize_measure(row)
                if not normalized:
                    continue
                measuretype, measurevalue, measureunit = normalized

                measure_date = parse_date(row.get("date_observation") or row.get("date_admission"))
                day, month, year = split_date(measure_date)
                if not month or not year:
                    continue

                key = (patientid, measuretype, str(measurevalue), measureunit, day, month, year)
                if key in seen:
                    continue
                seen.add(key)

                cur.execute(
                    insert_sql,
                    (
                        patientid,
                        measuretype,
                        measurevalue,
                        measureunit,
                        day,
                        month,
                        year,
                        patientid,
                        measuretype,
                        measurevalue,
                        measureunit,
                        day,
                        month,
                        year,
                    ),
                )
                inserted += cur.rowcount

        conn.commit()
        print(f"Measure rows inserted: {inserted}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

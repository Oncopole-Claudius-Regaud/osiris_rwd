from __future__ import annotations

import json
from datetime import date, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook


FILE_PATH = "/tmp/etl_iris/chirurgie.jsonl"


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

    if value.isdigit():
        timestamp = int(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000
        try:
            return datetime.fromtimestamp(timestamp).date()
        except (OSError, OverflowError, ValueError):
            return None

    for candidate in (value[:10], value):
        try:
            return date.fromisoformat(candidate)
        except ValueError:
            continue
    return None


def split_date(value):
    parsed = parse_date(value)
    if not parsed:
        return None, None, None
    return parsed.day, parsed.month, parsed.year


def get_loaded_patientids(cur):
    cur.execute("SELECT patientid FROM osiris_rwd.patient")
    return {str(row[0]).strip() for row in cur.fetchall() if row[0]}


def clean_text(value):
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def first_value(row, *keys):
    for key in keys:
        if key in row and row.get(key) is not None:
            return row.get(key)
    return None


def load_surgery():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        patientids = get_loaded_patientids(cur)
        cur.execute("TRUNCATE TABLE osiris_rwd.surgery RESTART IDENTITY")

        insert_sql = """
            INSERT INTO osiris_rwd.surgery (
                patientid,
                surgerydateday,
                surgerydatemonth,
                surgerydateyear,
                surgerycode,
                surgerytype,
                surgeryincenter,
                responseneoadjtreatment
            )
            SELECT %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1
                FROM osiris_rwd.surgery
                WHERE patientid = %s
                  AND surgerydateday IS NOT DISTINCT FROM %s
                  AND surgerydatemonth IS NOT DISTINCT FROM %s
                  AND surgerydateyear IS NOT DISTINCT FROM %s
                  AND surgerycode IS NOT DISTINCT FROM %s
                  AND surgerytype IS NOT DISTINCT FROM %s
            )
        """

        loaded = 0
        seen = set()
        with open(FILE_PATH, "r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)

                patientid = clean_text(first_value(row, "ipp_ocr", "P_CODE", "p_code"))
                if not patientid or patientid not in patientids:
                    continue

                surgery_date = parse_date(
                    first_value(row, "dat_deb_reel", "I_PLANNED_START", "i_planned_start")
                    or first_value(row, "dat_fin_reel", "I_PLANNED_END", "i_planned_end")
                )
                day, month, year = split_date(surgery_date)
                if not month or not year:
                    continue

                surgerycode = clean_text(first_value(row, "nom_interv", "I_LABEL", "i_label"))
                surgerytype = clean_text(first_value(row, "code_ccam", "IN_CODE", "in_code"))
                if not surgerycode and not surgerytype:
                    continue

                key = (patientid, day, month, year, surgerycode, surgerytype)
                if key in seen:
                    continue
                seen.add(key)

                params = (
                    patientid,
                    day,
                    month,
                    year,
                    surgerycode,
                    surgerytype,
                    True,
                    None,
                    patientid,
                    day,
                    month,
                    year,
                    surgerycode,
                    surgerytype,
                )
                cur.execute(insert_sql, params)
                loaded += cur.rowcount

        conn.commit()
        print(f"Surgery rows inserted: {loaded}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

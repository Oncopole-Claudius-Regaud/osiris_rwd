import json
from datetime import date, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

FILE_PATH = "/tmp/etl_iris/diagnostic.jsonl"


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


def get_loaded_patientids(cur):
    cur.execute("SELECT patientid FROM osiris_rwd.patient")
    return {row[0] for row in cur.fetchall()}


def load_relatedpathology():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    patientids = get_loaded_patientids(cur)
    cur.execute("TRUNCATE TABLE osiris_rwd.relatedpathology RESTART IDENTITY")

    sql = """
        INSERT INTO osiris_rwd.relatedpathology (
            patientid,
            relatedpathologycode,
            relateddiagnosisdateday,
            relateddiagnosisdatemonth,
            relateddiagnosisdateyear
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (
            patientid,
            relatedpathologycode,
            relateddiagnosisdateday,
            relateddiagnosisdatemonth,
            relateddiagnosisdateyear
        ) DO NOTHING;
    """

    seen = set()
    with open(FILE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            patientid = (row.get("ipp_ocr") or "").strip()
            if not patientid or patientid not in patientids:
                continue

            code = (row.get("code_cim") or row.get("diagnostic_source_value") or "").strip()
            diagnosis_date = parse_date(
                row.get("date_prelevement") or row.get("diagnostic_start_date")
            )
            if not code or not diagnosis_date:
                continue

            day, month, year = split_date(diagnosis_date)
            key = (patientid, code, day, month, year)
            if key in seen:
                continue
            seen.add(key)

            cur.execute(sql, key)

    conn.commit()
    cur.close()
    conn.close()

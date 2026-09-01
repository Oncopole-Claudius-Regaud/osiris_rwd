import json
from datetime import date, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

FILE_PATH = "/tmp/etl_iris/consentement.jsonl"


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


def parse_opposition(value):
    normalized = (value or "").strip().lower()
    if normalized in ("non", "no", "false", "f", "0", "consentement", "consent"):
        return False
    if normalized in ("oui", "yes", "true", "t", "1", "opposition", "refus"):
        return True
    return None


def get_loaded_patientids(cur):
    cur.execute("SELECT patientid FROM osiris_rwd.patient")
    return {row[0] for row in cur.fetchall()}


def load_opposition():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    patientids = get_loaded_patientids(cur)
    cur.execute("TRUNCATE TABLE osiris_rwd.opposition RESTART IDENTITY")

    sql = """
        INSERT INTO osiris_rwd.opposition (
            patientid,
            patientinformed,
            informationdateday,
            informationdatemonth,
            informationdateyear,
            oppositiondateday,
            oppositiondatemonth,
            oppositiondateyear,
            opposition
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (patientid) DO UPDATE SET
            patientinformed = EXCLUDED.patientinformed,
            informationdateday = EXCLUDED.informationdateday,
            informationdatemonth = EXCLUDED.informationdatemonth,
            informationdateyear = EXCLUDED.informationdateyear,
            oppositiondateday = EXCLUDED.oppositiondateday,
            oppositiondatemonth = EXCLUDED.oppositiondatemonth,
            oppositiondateyear = EXCLUDED.oppositiondateyear,
            opposition = EXCLUDED.opposition;
    """

    inserted = 0
    with open(FILE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            patientid = (row.get("ipp_ocr") or "").strip()
            if not patientid or patientid not in patientids:
                continue

            consentement = row.get("consentement")
            consent_date = parse_date(row.get("date_consentement"))
            opposition = parse_opposition(consentement)
            patient_informed = consent_date is not None or opposition is not None

            info_day, info_month, info_year = split_date(consent_date)
            if opposition:
                opposition_day, opposition_month, opposition_year = split_date(consent_date)
            else:
                opposition_day, opposition_month, opposition_year = None, None, None

            cur.execute(
                sql,
                (
                    patientid,
                    patient_informed,
                    info_day,
                    info_month,
                    info_year,
                    opposition_day,
                    opposition_month,
                    opposition_year,
                    opposition,
                ),
            )
            inserted += cur.rowcount

    conn.commit()
    print(f"Opposition rows inserted/updated: {inserted}")
    cur.close()
    conn.close()

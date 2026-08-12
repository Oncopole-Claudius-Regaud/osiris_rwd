import json
from datetime import date, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

FILE_PATH = "/tmp/etl_iris/patients.jsonl"
DATASET_ID = 1
COHORT_START_DATE = date(2020, 1, 1)


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
        pass

    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue

    return None


def split_birthdate(date_str):
    """
    Transforme 'YYYY-MM-DD' en (day, month, year).
    """
    birthdate = parse_date(date_str)
    if not birthdate:
        return None, None, None
    return birthdate.day, birthdate.month, birthdate.year


def get_eligible_patientids(cur):
    sql = """
        SELECT DISTINCT TRIM(ipp_ocr)
        FROM osiris.diagnostic
        WHERE ipp_ocr IS NOT NULL
          AND TRIM(ipp_ocr) <> ''
          AND date_prelevement >= %s
    """
    cur.execute(sql, (COHORT_START_DATE,))
    return {row[0] for row in cur.fetchall()}


def load():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()
    eligible_patientids = get_eligible_patientids(cur)

    gender_mapping = {
        "Masculin": "Male",
        "Feminin": "Female",
        "Féminin": "Female",
        "FÃ©minin": "Female",
        "Male": "Male",
        "Female": "Female",
    }

    sql = """
        INSERT INTO osiris_rwd.patient (
            patientid,
            birthdateday,
            birthdatemonth,
            birthdateyear,
            biologicalsex,
            patientupdate,
            datasetid
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (patientid) DO UPDATE SET
            birthdateday = EXCLUDED.birthdateday,
            birthdatemonth = EXCLUDED.birthdatemonth,
            birthdateyear = EXCLUDED.birthdateyear,
            biologicalsex = EXCLUDED.biologicalsex,
            patientupdate = EXCLUDED.patientupdate,
            datasetid = EXCLUDED.datasetid;
    """

    with open(FILE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            patient_id = (r.get("ipp_ocr") or "").strip()
            if not patient_id or patient_id not in eligible_patientids:
                continue

            birth_day, birth_month, birth_year = split_birthdate(r.get("date_of_birth"))
            patient_update = parse_date(r.get("patientupdate"))

            cur.execute(
                sql,
                (
                    patient_id,
                    birth_day,
                    birth_month,
                    birth_year,
                    gender_mapping.get(r.get("gender"), "Unknown"),
                    patient_update,
                    DATASET_ID,
                ),
            )

    conn.commit()
    cur.close()
    conn.close()

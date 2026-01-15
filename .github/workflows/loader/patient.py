import json
from airflow.providers.postgres.hooks.postgres import PostgresHook

FILE_PATH = "/tmp/etl_iris/patients.jsonl"


def split_birthdate(date_str):
    """
    Transforme 'YYYY-MM-DD' en (day, month, year)
    """
    if not date_str:
        return None, None, None
    try:
        year, month, day = date_str.split("-")
        return int(day), int(month), int(year)
    except Exception:
        return None, None, None


def load():
    hook = PostgresHook(postgres_conn_id="osrisis_rw")
    conn = hook.get_conn()
    cur = conn.cursor()

    gender_mapping = {
        "Masculin": "Male",
        "Féminin": "Female"
    }

    sql = """
        INSERT INTO osrisis_rwd.patient (
            patientid,
            birthdateday,
            birthdatemonth,
            birthdateyear,
            biologicalsex,
            patientupdate
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (patientid) DO UPDATE SET
            birthdateday = EXCLUDED.birthdateday,
            birthdatemonth = EXCLUDED.birthdatemonth,
            birthdateyear = EXCLUDED.birthdateyear,
            biologicalsex = EXCLUDED.biologicalsex,
            patientupdatedate = COALESCE(NULLIF(EXCLUDED.patientupdate, ''), osiris_rwd.patient.patientupdate);
    """

    with open(FILE_PATH, "r") as f:
        for line in f:
            r = json.loads(line)
            birth_day, birth_month, birth_year = split_birthdate(r.get("date_of_birth"))
            patient_update = r.get("patientupdate")
            if not patient_update or patient_update.strip() == "":
                patient_update = None

            cur.execute(sql, (
                r.get("ipp_ocr"),
                birth_day,
                birth_month,
                birth_year,
                gender_mapping.get(r.get("gender"), "Unknown"),
                patient_update
            ))

    conn.commit()
    cur.close()
    conn.close()

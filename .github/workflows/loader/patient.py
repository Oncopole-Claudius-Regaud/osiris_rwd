import json
from airflow.providers.postgres.hooks.postgres import PostgresHook

FILE_PATH = "/tmp/etl_iris/patients.jsonl"

def load():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    gender_mapping = {
        "Masculin": "Male",
        "Féminin": "Female"
    }

    sql = """
        INSERT INTO osrisis_rwd.patient (
            PatientId,
            BirthDateDay,
            BirthDateMonth,
            BirthDateYear,
            BiologicalSex,
            PatientUpdateDate
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (PatientId) DO UPDATE SET
            BirthDateDay = EXCLUDED.BirthDateDay,
            BirthDateMonth = EXCLUDED.BirthDateMonth,
            BirthDateYear = EXCLUDED.BirthDateYear,
            BiologicalSex = EXCLUDED.BiologicalSex,
            PatientUpdate = EXCLUDED.PatientUpdate;
    """

    with open(FILE_PATH, "r") as f:
        for line in f:
            r = json.loads(line)
            dob = r.get("date_of_birth")

            y = m = d = None
            if dob:
                try:
                    y, m, d = dob.split("-")
                except ValueError:
                    pass

            cur.execute(sql, (
                r.get("ipp_ocr"),
                d,
                m,
                y,
                gender_mapping.get(r.get("gender"), "Unknown"),
                r.get("patientupdate")
            ))

    conn.commit()
    cur.close()
    conn.close()

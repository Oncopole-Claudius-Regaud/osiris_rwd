import json
import re
from collections import defaultdict
from datetime import date, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook


FILE_PATH = "/tmp/etl_iris/diagnostic.jsonl"
CIM_CODE_PATTERN = re.compile(r"^[A-Z][0-9]{2}(?:[.]?[0-9A-Z]{1,2})?$", re.I)
MORPHOLOGY_PATTERN = re.compile(r"^[0-9]{4}/[0-9]$")


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


def normalize_cim_code(value):
    code = (value or "").strip().upper()
    if not code:
        return None
    code = code.replace(",", ".")
    if CIM_CODE_PATTERN.match(code):
        return code
    return None


def row_cim_code(row):
    for key in ("code_cim", "diagnostic_code", "cancerdiagnosiscode", "cim10_code"):
        code = normalize_cim_code(row.get(key))
        if code:
            return code
    return None


def row_diagnosis_date(row):
    return parse_date(
        row.get("date_prelevement")
        or row.get("diagnostic_start_date")
        or row.get("date_diagnostic")
        or row.get("diagnosis_date")
    )


def row_diagnosis_method(row):
    value = (
        row.get("cancerdiagnosismethod")
        or row.get("diagnostic_method")
        or row.get("diagnosis_method")
    )
    value = (value or "").strip()
    return value or None


def only_digits(value):
    return re.sub(r"\D", "", str(value or ""))


def row_morphology_code(row):
    direct = (row.get("morphologycode") or row.get("morphology_code") or "").strip()
    if direct:
        direct = direct.replace("\\", "/")
        if MORPHOLOGY_PATTERN.match(direct):
            return direct

    code_4 = only_digits(row.get("code_morph_4") or row.get("morphology_code_4"))
    code_5 = only_digits(row.get("code_morph_5") or row.get("morphology_behavior"))
    if len(code_4) == 4 and len(code_5) >= 1:
        return f"{code_4}/{code_5[0]}"

    source = only_digits(
        row.get("code_morphologique_source")
        or row.get("morphological_code")
    )
    if len(source) >= 5:
        return f"{source[:4]}/{source[4]}"
    return None


def row_laterality(row):
    value = (
        row.get("laterality")
        or row.get("lateralite")
        or row.get("latéralité")
        or ""
    )
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    if normalized in ("left", "gauche", "l", "1"):
        return "LEFT"
    if normalized in ("right", "droit", "droite", "r", "2"):
        return "RIGHT"
    if normalized in ("bilateral", "bilatéral", "bilaterale", "bilateral", "b", "3"):
        return "BILATERAL"
    if normalized in ("unknown", "inconnu", "inconnue", "u", "9"):
        return "UNKNOWN"
    return "UNKNOWN"


def load_primarycancer():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        patientids = get_loaded_patientids(cur)
        cur.execute("TRUNCATE TABLE osiris_rwd.primarycancer RESTART IDENTITY CASCADE")

        rows_by_patient = defaultdict(dict)
        with open(FILE_PATH, "r", encoding="utf-8") as f:
            for line in f:
                row = json.loads(line)
                patientid = (row.get("ipp_ocr") or "").strip()
                if not patientid or patientid not in patientids:
                    continue

                code = row_cim_code(row)
                diagnosis_date = row_diagnosis_date(row)
                if not code or not diagnosis_date:
                    continue

                day, month, year = split_date(diagnosis_date)
                key = (code, day, month, year)
                rows_by_patient[patientid].setdefault(
                    key,
                    {
                        "patientid": patientid,
                        "code": code,
                        "date": diagnosis_date,
                        "day": day,
                        "month": month,
                        "year": year,
                        "method": row_diagnosis_method(row),
                        "morphologycode": row_morphology_code(row),
                        "laterality": row_laterality(row),
                    },
                )

        insert_sql = """
            INSERT INTO osiris_rwd.primarycancer (
                patientid,
                cancerorder,
                cancerdiagnosisdateday,
                cancerdiagnosisdatemonth,
                cancerdiagnosisdateyear,
                cancerdiagnosismethod,
                cancerdiagnosiscode,
                cancerdiagnosisincenter,
                cancercareincenter,
                topographygroup,
                topographycode,
                morphologygroup,
                morphologycode,
                laterality
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        inserted = 0
        for patientid in sorted(rows_by_patient):
            rows = sorted(
                rows_by_patient[patientid].values(),
                key=lambda item: (item["date"], item["code"]),
            )
            for cancer_order, row in enumerate(rows, start=1):
                cur.execute(
                    insert_sql,
                    (
                        row["patientid"],
                        cancer_order,
                        row["day"],
                        row["month"],
                        row["year"],
                        row["method"],
                        row["code"],
                        True,
                        True,
                        None,
                        None,
                        "CIM-O-3.2" if row["morphologycode"] else None,
                        row["morphologycode"],
                        row["laterality"],
                    ),
                )
                inserted += 1

        conn.commit()
        print(f"PrimaryCancer rows inserted: {inserted}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

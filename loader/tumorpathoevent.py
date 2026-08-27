from datetime import date, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook


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


def load_tumorpathoevent():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("TRUNCATE TABLE osiris_rwd.tumorpathoevent RESTART IDENTITY CASCADE")

        select_sql = """
            SELECT DISTINCT
                pc.primarycancerid,
                d.date_prelevement,
                d.morphologycode
            FROM osiris_rwd.primarycancer pc
            JOIN osiris_rwd.patient p
              ON p.patientid = pc.patientid
            JOIN osiris.diagnostic d
              ON d.ipp_ocr = pc.patientid
             AND (
                    d.cancerdiagnosiscode = pc.cancerdiagnosiscode
                 OR d.code_cim = pc.cancerdiagnosiscode
                 OR split_part(d.cancerdiagnosiscode, '.', 1) = pc.cancerdiagnosiscode
                 OR split_part(d.code_cim, '.', 1) = pc.cancerdiagnosiscode
                 OR d.cancerdiagnosiscode = split_part(pc.cancerdiagnosiscode, '.', 1)
                 OR d.code_cim = split_part(pc.cancerdiagnosiscode, '.', 1)
             )
            WHERE d.morphologycode IS NOT NULL
              AND TRIM(d.morphologycode) <> ''
        """
        cur.execute(select_sql)
        rows = cur.fetchall()

        insert_sql = """
            INSERT INTO osiris_rwd.tumorpathoevent (
                primarycancerid,
                tumeventtype,
                tumeventmentiondateday,
                tumeventmentiondatemonth,
                tumeventmentiondateyear,
                tumeventdiagmethod,
                tumeventmorphologycode
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (
                primarycancerid,
                tumeventtype,
                tumeventmentiondateday,
                tumeventmentiondatemonth,
                tumeventmentiondateyear,
                tumeventdiagmethod,
                tumeventmorphologycode
            ) DO NOTHING
        """

        inserted = 0
        seen = set()
        for primarycancerid, mention_date, morphologycode in rows:
            day, month, year = split_date(mention_date)
            morph = (morphologycode or "").strip()
            if not morph:
                continue
            key = (
                primarycancerid,
                "PRIMARY_TUMOR",
                day,
                month,
                year,
                "PATHOLOGY",
                morph,
            )
            if key in seen:
                continue
            seen.add(key)
            cur.execute(insert_sql, key)
            inserted += 1

        conn.commit()
        print(f"TumorPathoEvent rows inserted: {inserted}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

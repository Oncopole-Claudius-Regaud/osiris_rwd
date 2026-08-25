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


def load_metastasis():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("TRUNCATE TABLE osiris_rwd.metastasis RESTART IDENTITY")

        select_sql = """
            WITH stade AS (
                SELECT
                    s.ipp,
                    s.code_cim,
                    s.document_date,
                    s.m,
                    s.metastasis_detected,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.ipp, s.code_cim, s.document_date
                        ORDER BY
                            s.stage_confidence DESC NULLS LAST,
                            s.last_update DESC NULLS LAST,
                            s.document_date DESC NULLS LAST
                    ) AS rn
                FROM sein.ipp_stade s
                JOIN osiris_rwd.patient p
                  ON p.patientid = s.ipp
                WHERE COALESCE(s.metastasis_detected, false) IS TRUE
                   OR LOWER(COALESCE(s.m, '')) LIKE 'm1%'
            )
            SELECT
                tpe.tumorpathoeventid,
                st.document_date
            FROM stade st
            JOIN osiris_rwd.primarycancer pc
              ON pc.patientid = st.ipp
             AND (
                    pc.cancerdiagnosiscode = st.code_cim
                 OR split_part(pc.cancerdiagnosiscode, '.', 1) = st.code_cim
                 OR pc.cancerdiagnosiscode = split_part(st.code_cim, '.', 1)
             )
            JOIN osiris_rwd.tumorpathoevent tpe
              ON tpe.primarycancerid = pc.primarycancerid
            WHERE st.rn = 1
        """
        cur.execute(select_sql)
        rows = cur.fetchall()

        insert_sql = """
            INSERT INTO osiris_rwd.metastasis (
                tumorpathoeventid,
                metastasisdiscoverydateday,
                metastasisdiscoverydatemonth,
                metastasisdiscoverydateyear,
                metastasistopographycode
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (
                tumorpathoeventid,
                metastasisdiscoverydateday,
                metastasisdiscoverydatemonth,
                metastasisdiscoverydateyear,
                metastasistopographycode
            ) DO NOTHING
        """

        inserted = 0
        seen = set()
        for tumorpathoeventid, document_date in rows:
            day, month, year = split_date(document_date)
            key = (tumorpathoeventid, day, month, year, None)
            if key in seen:
                continue
            seen.add(key)
            cur.execute(insert_sql, key)
            inserted += 1

        conn.commit()
        print(f"Metastasis rows inserted: {inserted}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

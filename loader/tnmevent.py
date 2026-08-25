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


def normalize_stage_value(value):
    value = (value or "").strip()
    if not value:
        return None
    return value.upper()


def infer_tnm_type(tnm_raw, tvalue, nvalue, mvalue):
    raw = (tnm_raw or "").strip().lower()
    values = " ".join(v for v in (tvalue, nvalue, mvalue) if v).lower()
    haystack = f"{raw} {values}".strip()
    if not haystack:
        return None
    if "yp" in haystack:
        return "yp"
    if "yc" in haystack:
        return "yc"
    if "p" in haystack:
        return "p"
    if "c" in haystack:
        return "c"
    return None


def load_tnmevent():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("TRUNCATE TABLE osiris_rwd.tnmevent RESTART IDENTITY")

        select_sql = """
            WITH stade AS (
                SELECT
                    s.ipp,
                    s.code_cim,
                    s.tnm_raw,
                    s.t,
                    s.n,
                    s.m,
                    s.document_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.ipp, s.code_cim, s.t, s.n, s.m, s.document_date
                        ORDER BY
                            s.stage_confidence DESC NULLS LAST,
                            s.last_update DESC NULLS LAST,
                            s.document_date DESC NULLS LAST
                    ) AS rn
                FROM sein.ipp_stade s
                JOIN osiris_rwd.patient p
                  ON p.patientid = s.ipp
                WHERE COALESCE(NULLIF(TRIM(s.t), ''), NULLIF(TRIM(s.n), ''), NULLIF(TRIM(s.m), '')) IS NOT NULL
            )
            SELECT
                pc.primarycancerid,
                st.tnm_raw,
                st.t,
                st.n,
                st.m,
                st.document_date
            FROM stade st
            JOIN osiris_rwd.primarycancer pc
              ON pc.patientid = st.ipp
             AND (
                    pc.cancerdiagnosiscode = st.code_cim
                 OR split_part(pc.cancerdiagnosiscode, '.', 1) = st.code_cim
                 OR pc.cancerdiagnosiscode = split_part(st.code_cim, '.', 1)
             )
            WHERE st.rn = 1
        """
        cur.execute(select_sql)
        rows = cur.fetchall()

        insert_sql = """
            INSERT INTO osiris_rwd.tnmevent (
                primarycancerid,
                tvalue,
                nvalue,
                mvalue,
                tnmtype,
                tnmeventdateday,
                tnmeventdatemonth,
                tnmeventdateyear
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (
                primarycancerid,
                tvalue,
                nvalue,
                mvalue,
                tnmtype,
                tnmeventdateday,
                tnmeventdatemonth,
                tnmeventdateyear
            ) DO NOTHING
        """

        inserted = 0
        seen = set()
        for primarycancerid, tnm_raw, tvalue, nvalue, mvalue, document_date in rows:
            tvalue = normalize_stage_value(tvalue)
            nvalue = normalize_stage_value(nvalue)
            mvalue = normalize_stage_value(mvalue)
            if not any((tvalue, nvalue, mvalue)):
                continue

            tnmtype = infer_tnm_type(tnm_raw, tvalue, nvalue, mvalue)
            day, month, year = split_date(document_date)
            key = (primarycancerid, tvalue, nvalue, mvalue, tnmtype, day, month, year)
            if key in seen:
                continue
            seen.add(key)
            cur.execute(insert_sql, key)
            inserted += 1

        conn.commit()
        print(f"TNMEvent rows inserted: {inserted}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

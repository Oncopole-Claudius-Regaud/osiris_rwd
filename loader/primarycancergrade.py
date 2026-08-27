from airflow.providers.postgres.hooks.postgres import PostgresHook


def normalize_grade(value):
    if value is None:
        return None
    value = str(value).strip()
    if not value:
        return None
    lowered = value.lower()
    for prefix in ("grade", "sbr"):
        lowered = lowered.replace(prefix, "")
    cleaned = lowered.strip(" :;-").upper()
    return cleaned or None


def load_primarycancergrade():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("TRUNCATE TABLE osiris_rwd.primarycancergrade RESTART IDENTITY")

        select_sql = """
            WITH stade AS (
                SELECT
                    s.ipp,
                    s.code_cim,
                    s.grade_sbr,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.ipp, s.code_cim, s.grade_sbr
                        ORDER BY
                            s.last_update DESC NULLS LAST,
                            s.document_date DESC NULLS LAST
                    ) AS rn
                FROM sein.ipp_stade s
                JOIN osiris_rwd.patient p
                  ON p.patientid = s.ipp
                WHERE NULLIF(TRIM(s.grade_sbr::text), '') IS NOT NULL
            )
            SELECT
                pc.primarycancerid,
                st.grade_sbr
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
            INSERT INTO osiris_rwd.primarycancergrade (
                primarycancerid,
                histologicalgradetype,
                histologicalgradevalue
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (
                primarycancerid,
                histologicalgradetype,
                histologicalgradevalue
            ) DO NOTHING
        """

        inserted = 0
        seen = set()
        for primarycancerid, grade_sbr in rows:
            grade_value = normalize_grade(grade_sbr)
            if not grade_value:
                continue
            key = (primarycancerid, "SBR", grade_value)
            if key in seen:
                continue
            seen.add(key)
            cur.execute(insert_sql, key)
            inserted += 1

        conn.commit()
        print(f"PrimaryCancerGrade rows inserted: {inserted}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

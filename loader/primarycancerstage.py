import re

from airflow.providers.postgres.hooks.postgres import PostgresHook


STAGE_PREFIX_RE = re.compile(r"^\s*stage\s+", re.I)


def normalize_stage(value):
    value = (value or "").strip()
    if not value:
        return None
    value = STAGE_PREFIX_RE.sub("", value).strip()
    value = value.replace(" ", "").upper()
    return value or None


def load_primarycancerstage():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("TRUNCATE TABLE osiris_rwd.primarycancerstage RESTART IDENTITY")

        select_sql = """
            WITH stade AS (
                SELECT
                    s.ipp,
                    s.code_cim,
                    s.stage,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.ipp, s.code_cim, s.stage
                        ORDER BY
                            s.stage_confidence DESC NULLS LAST,
                            s.last_update DESC NULLS LAST,
                            s.document_date DESC NULLS LAST
                    ) AS rn
                FROM sein.ipp_stade s
                JOIN osiris_rwd.patient p
                  ON p.patientid = s.ipp
                WHERE NULLIF(TRIM(s.stage), '') IS NOT NULL
            )
            SELECT
                pc.primarycancerid,
                st.stage
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
            INSERT INTO osiris_rwd.primarycancerstage (
                primarycancerid,
                stagetype,
                stagevalue
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (
                primarycancerid,
                stagetype,
                stagevalue
            ) DO NOTHING
        """

        inserted = 0
        seen = set()
        for primarycancerid, stage in rows:
            stagevalue = normalize_stage(stage)
            if not stagevalue:
                continue
            key = (primarycancerid, "TNM", stagevalue)
            if key in seen:
                continue
            seen.add(key)
            cur.execute(insert_sql, key)
            inserted += 1

        conn.commit()
        print(f"PrimaryCancerStage rows inserted: {inserted}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

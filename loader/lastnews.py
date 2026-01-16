from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import date


def load_lastnews():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        # --------------------
        # TRUNCATE
        # --------------------
        cur.execute("TRUNCATE TABLE osiris_rwd.lastnews")

        # --------------------
        # Lecture source
        # --------------------
        select_sql = """
            SELECT
                ipp_ocr,
                date_of_death,
                date_derniere_nouvelle
            FROM datamart_oeci_survie.v_date_derniere_nouvelle_combinee
        """
        cur.execute(select_sql)
        rows = cur.fetchall()

        today = date.today()

        # --------------------
        # INSERT
        # --------------------
        insert_sql = """
            INSERT INTO osiris_rwd.lastnews (
                patientid,
                vitalstatus,
                vitalstatusupdateday,
                vitalstatusupdatemonth,
                vitalstatusupdateyear,
                deathdateday,
                deathdatemonth,
                deathdateyear,
                lastvisitdateday,
                lastvisitdatemonth,
                lastvisitdateyear
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for patientid, date_of_death, date_derniere_nouvelle in rows:
            vitalstatus = "vivant" if date_of_death is None else "décédé"

            death_day = date_of_death.day if date_of_death else None
            death_month = date_of_death.month if date_of_death else None
            death_year = date_of_death.year if date_of_death else None

            last_day = date_derniere_nouvelle.day if date_derniere_nouvelle else None
            last_month = date_derniere_nouvelle.month if date_derniere_nouvelle else None
            last_year = date_derniere_nouvelle.year if date_derniere_nouvelle else None

            cur.execute(
                insert_sql,
                (
                    patientid,
                    vitalstatus,
                    today.day,
                    today.month,
                    today.year,
                    death_day,
                    death_month,
                    death_year,
                    last_day,
                    last_month,
                    last_year,
                )
            )

        conn.commit()

    finally:
        cur.close()
        conn.close()

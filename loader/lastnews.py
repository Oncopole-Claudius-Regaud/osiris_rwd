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


def load_lastnews():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("TRUNCATE TABLE osiris_rwd.lastnews")

        select_sql = """
            SELECT
                p.patientid,
                dc.date_deces_combinee AS date_of_death,
                da.derniere_date_admission AS last_visit_date,
                COALESCE(dc.date_deces_combinee, da.derniere_date_admission) AS last_contact_date,
                CASE
                    WHEN dc.date_deces_combinee IS NOT NULL THEN 'Deceased'
                    WHEN da.derniere_date_admission >= date_trunc('year', CURRENT_DATE)::date
                     AND da.derniere_date_admission < (date_trunc('year', CURRENT_DATE) + INTERVAL '1 year')::date
                        THEN 'Alive'
                    ELSE 'Unknown'
                END AS vitalstatus
            FROM osiris_rwd.patient p
            LEFT JOIN datamart_oeci_survie.v_date_deces_combinee dc
                ON dc.ipp_ocr = p.patientid
            LEFT JOIN datamart_oeci_survie.v_derniere_admission da
                ON da.ipp_ocr = p.patientid
        """
        cur.execute(select_sql)
        rows = cur.fetchall()

        update_day, update_month, update_year = split_date(date.today())

        insert_sql = """
            INSERT INTO osiris_rwd.lastnews (
                patientid,
                vitalstatus,
                lastvisitdateday,
                lastvisitdatemonth,
                lastvisitdateyear,
                deathdateday,
                deathdatemonth,
                deathdateyear,
                lastcontactdateday,
                lastcontactdatemonth,
                lastcontactdateyear,
                vitalstatusupdateday,
                vitalstatusupdatemonth,
                vitalstatusupdateyear
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (patientid) DO UPDATE SET
                vitalstatus = EXCLUDED.vitalstatus,
                lastvisitdateday = EXCLUDED.lastvisitdateday,
                lastvisitdatemonth = EXCLUDED.lastvisitdatemonth,
                lastvisitdateyear = EXCLUDED.lastvisitdateyear,
                deathdateday = EXCLUDED.deathdateday,
                deathdatemonth = EXCLUDED.deathdatemonth,
                deathdateyear = EXCLUDED.deathdateyear,
                lastcontactdateday = EXCLUDED.lastcontactdateday,
                lastcontactdatemonth = EXCLUDED.lastcontactdatemonth,
                lastcontactdateyear = EXCLUDED.lastcontactdateyear,
                vitalstatusupdateday = EXCLUDED.vitalstatusupdateday,
                vitalstatusupdatemonth = EXCLUDED.vitalstatusupdatemonth,
                vitalstatusupdateyear = EXCLUDED.vitalstatusupdateyear
        """

        for patientid, date_of_death, last_visit_date, last_contact_date, vitalstatus in rows:
            if not patientid:
                continue

            death_day, death_month, death_year = split_date(date_of_death)
            visit_day, visit_month, visit_year = split_date(last_visit_date)
            contact_day, contact_month, contact_year = split_date(last_contact_date)

            cur.execute(
                insert_sql,
                (
                    patientid,
                    vitalstatus,
                    visit_day,
                    visit_month,
                    visit_year,
                    death_day,
                    death_month,
                    death_year,
                    contact_day,
                    contact_month,
                    contact_year,
                    update_day,
                    update_month,
                    update_year,
                ),
            )

        conn.commit()

    finally:
        cur.close()
        conn.close()

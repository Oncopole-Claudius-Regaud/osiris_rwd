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


def map_vitalstatus(value):
    status = (value or "").strip().upper()
    if status in ("DECEDE", "DÉCÉDÉ", "DÃ©CÃ©DÃ©", "DÃ‰CÃ‰DÃ‰"):
        return "Deceased"
    if status == "VIVANT":
        return "Alive"
    if status == "PDV":
        return "Unknown"
    return "Unknown"


def load_lastnews():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("TRUNCATE TABLE osiris_rwd.lastnews")

        select_sql = """
            SELECT DISTINCT ON (src.ipp_ocr)
                src.ipp_ocr,
                dnc.date_of_death,
                dnc.date_derniere_nouvelle,
                src.statut_vital
            FROM datamart_oeci_survie.v_statut_vital src
            LEFT JOIN datamart_oeci_survie.v_date_derniere_nouvelle_combinee dnc
                ON dnc.ipp_ocr = src.ipp_ocr
            WHERE EXISTS (
                SELECT 1
                FROM osiris_rwd.patient p
                WHERE p.patientid = src.ipp_ocr
            )
            ORDER BY
                src.ipp_ocr,
                src.date_diagnostic DESC NULLS LAST,
                dnc.date_derniere_nouvelle DESC NULLS LAST
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

        for patientid, date_of_death, date_derniere_nouvelle, statut_vital in rows:
            if not patientid:
                continue

            death_day, death_month, death_year = split_date(date_of_death)
            vitalstatus = map_vitalstatus(statut_vital)
            visit_day, visit_month, visit_year = split_date(date_derniere_nouvelle)
            contact_day, contact_month, contact_year = split_date(date_derniere_nouvelle)

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

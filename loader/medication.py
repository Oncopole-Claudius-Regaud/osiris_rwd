from __future__ import annotations

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


def clean_text(value):
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def load_medication():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("TRUNCATE TABLE osiris_rwd.medication RESTART IDENTITY")

        select_sql = """
            SELECT
                o.ipp_ocr AS patientid,
                NULLIF(TRIM(o.fac_code), '') AS moleculecode,
                NULLIF(TRIM(o.libelle_produit), '') AS moleculename,
                o.date_ordonnance AS moleculedate,
                TRUE AS moleculeincenter,
                NULL::boolean AS moleculechange,
                NULL::boolean AS earlyaccess,
                NULL::boolean AS clinicaltrial
            FROM osiris.ordonnance_sortie o
            JOIN osiris_rwd.patient p
              ON p.patientid = o.ipp_ocr
            WHERE o.date_ordonnance IS NOT NULL
              AND (
                    NULLIF(TRIM(o.fac_code), '') IS NOT NULL
                 OR NULLIF(TRIM(o.libelle_produit), '') IS NOT NULL
              )

            UNION ALL

            SELECT
                c.num_doss AS patientid,
                NULL::text AS moleculecode,
                COALESCE(
                    NULLIF(TRIM(c.lib_dci), ''),
                    NULLIF(TRIM(c.cp_lib_med_presc), ''),
                    NULLIF(TRIM(c.lib_ucd), ''),
                    NULLIF(TRIM(c.nom_pdt), '')
                ) AS moleculename,
                c.dat_admini AS moleculedate,
                TRUE AS moleculeincenter,
                NULL::boolean AS moleculechange,
                NULL::boolean AS earlyaccess,
                NULL::boolean AS clinicaltrial
            FROM osiris.chimiotherapie c
            JOIN osiris_rwd.patient p
              ON p.patientid = c.num_doss
            WHERE c.dat_admini IS NOT NULL
              AND COALESCE(
                    NULLIF(TRIM(c.lib_dci), ''),
                    NULLIF(TRIM(c.cp_lib_med_presc), ''),
                    NULLIF(TRIM(c.lib_ucd), ''),
                    NULLIF(TRIM(c.nom_pdt), '')
              ) IS NOT NULL
        """
        cur.execute(select_sql)
        rows = cur.fetchall()

        insert_sql = """
            INSERT INTO osiris_rwd.medication (
                patientid,
                moleculecode,
                moleculename,
                moleculedateday,
                moleculedatemonth,
                moleculedateyear,
                moleculeincenter,
                moleculechange,
                earlyaccess,
                clinicaltrial
            )
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1
                FROM osiris_rwd.medication
                WHERE patientid = %s
                  AND moleculecode IS NOT DISTINCT FROM %s
                  AND moleculename IS NOT DISTINCT FROM %s
                  AND moleculedateday IS NOT DISTINCT FROM %s
                  AND moleculedatemonth IS NOT DISTINCT FROM %s
                  AND moleculedateyear IS NOT DISTINCT FROM %s
            )
        """

        inserted = 0
        seen = set()
        for (
            patientid,
            moleculecode,
            moleculename,
            moleculedate,
            moleculeincenter,
            moleculechange,
            earlyaccess,
            clinicaltrial,
        ) in rows:
            patientid = clean_text(patientid)
            moleculecode = clean_text(moleculecode)
            moleculename = clean_text(moleculename)
            day, month, year = split_date(moleculedate)

            if not patientid or not month or not year:
                continue
            if not moleculecode and not moleculename:
                continue

            key = (patientid, moleculecode, moleculename, day, month, year)
            if key in seen:
                continue
            seen.add(key)

            cur.execute(
                insert_sql,
                (
                    patientid,
                    moleculecode,
                    moleculename,
                    day,
                    month,
                    year,
                    moleculeincenter,
                    moleculechange,
                    earlyaccess,
                    clinicaltrial,
                    patientid,
                    moleculecode,
                    moleculename,
                    day,
                    month,
                    year,
                ),
            )
            inserted += cur.rowcount

        conn.commit()
        print(f"Medication rows inserted: {inserted}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

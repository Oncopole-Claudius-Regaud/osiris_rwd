from airflow.providers.postgres.hooks.postgres import PostgresHook


CCAM_RADIOTHERAPY_TYPE = {
    "ZZNL047": "47479005",
    "ZZNL045": "50632006",
    "ZZNL052": "395096001",
    "ZZNL058": "395096001",
    "ZZNL059": "395096001",
    "ZZNL060": "395096001",
    "ZZNL049": "395096001",
    "ZZNL055": "395096001",
    "ZANL001": "395096001",
}
BRACHYTHERAPY_SNOMED = "152198000"
GENERIC_RADIOTHERAPY_SNOMED = "1287742003"


def radiotherapy_type_sql():
    cases = [
        f"WHEN upper(trim(cc.code_ccam)) = '{code}' THEN '{snomed}'"
        for code, snomed in CCAM_RADIOTHERAPY_TYPE.items()
    ]
    return "\n                    ".join(cases)


def load_radiotherapy():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("TRUNCATE TABLE osiris_rwd.radiotherapy RESTART IDENTITY")

        select_sql = f"""
            SELECT DISTINCT
                ch.ipp_ocr::varchar AS patientid,
                COALESCE(ch.dat_deb_reel, ch.dat_fin_reel)::date AS radiation_date,
                upper(trim(ch.code_ccam)) AS radiotherapycode,
                CASE
                    {radiotherapy_type_sql()}
                    WHEN NULLIF(trim(cc.chir_list_curietherapie::text), '') = '1' THEN '{BRACHYTHERAPY_SNOMED}'
                    ELSE '{GENERIC_RADIOTHERAPY_SNOMED}'
                END AS radiotherapytype
            FROM osiris.chirurgie ch
            JOIN osiris_rwd.patient p
              ON p.patientid = ch.ipp_ocr::varchar
            LEFT JOIN ref_source_externe.ccam cc
              ON upper(trim(cc.code_ccam)) = upper(trim(ch.code_ccam))
            WHERE ch.code_ccam IS NOT NULL
              AND trim(ch.code_ccam) <> ''
              AND COALESCE(ch.dat_deb_reel, ch.dat_fin_reel) IS NOT NULL
              AND (
                    NULLIF(trim(cc.chir_list_curietherapie::text), '') = '1'
                 OR upper(trim(ch.code_ccam)) = ANY(%s)
              )
        """
        cur.execute(select_sql, (list(CCAM_RADIOTHERAPY_TYPE),))
        rows = cur.fetchall()

        insert_sql = """
            INSERT INTO osiris_rwd.radiotherapy (
                patientid,
                radiationdateday,
                radiationdatemonth,
                radiationdateyear,
                radiotherapycode,
                radiotherapytype,
                radiotherapyincenter,
                radiotherapytopography
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        inserted = 0
        for patientid, radiation_date, radiotherapycode, radiotherapytype in rows:
            cur.execute(
                insert_sql,
                (
                    patientid,
                    radiation_date.day,
                    radiation_date.month,
                    radiation_date.year,
                    radiotherapycode,
                    radiotherapytype,
                    True,
                    None,
                ),
            )
            inserted += 1

        conn.commit()
        print(f"Radiotherapy rows inserted: {inserted}; source rows selected: {len(rows)}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_tumorpathoevent():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("TRUNCATE TABLE osiris_rwd.tumorpathoevent RESTART IDENTITY CASCADE")
        conn.commit()
        print(
            "TumorPathoEvent rows inserted: 0; "
            "PRIMARY_TUMOR is not an OSIRIS RWD TumEventType. "
            "Only recurrence, metastasis or transformation should populate this table."
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

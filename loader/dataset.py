from datetime import date

from airflow.providers.postgres.hooks.postgres import PostgresHook

DATASET_ID = 1
ORIGIN_CENTER_ID = "310782347"


def update_dataset():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cursor = conn.cursor()

    sql = """
        INSERT INTO osiris_rwd.dataset (datasetid, origincenterid, datasetupdatedate)
        VALUES (%s, %s, %s)
        ON CONFLICT (datasetid) DO UPDATE SET
            origincenterid = EXCLUDED.origincenterid,
            datasetupdatedate = EXCLUDED.datasetupdatedate;
    """
    cursor.execute(sql, (DATASET_ID, ORIGIN_CENTER_ID, date.today()))
    conn.commit()
    cursor.close()
    conn.close()

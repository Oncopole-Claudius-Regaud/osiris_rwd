from datetime import date

from airflow.providers.postgres.hooks.postgres import PostgresHook

DATASET_ID = 1
ORIGIN_CENTER_ID = "310782347"


def update_dataset():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cursor = conn.cursor()

    update_sql = """
        UPDATE osiris_rwd.dataset
        SET origincenterid = %s,
            datasetupdatedate = %s
        WHERE datasetid = %s
    """

    insert_sql = """
        INSERT INTO osiris_rwd.dataset (datasetid, origincenterid, datasetupdatedate)
        SELECT %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1
            FROM osiris_rwd.dataset
            WHERE datasetid = %s
        )
    """

    today = date.today()
    cursor.execute(update_sql, (ORIGIN_CENTER_ID, today, DATASET_ID))
    cursor.execute(insert_sql, (DATASET_ID, ORIGIN_CENTER_ID, today, DATASET_ID))
    conn.commit()
    cursor.close()
    conn.close()

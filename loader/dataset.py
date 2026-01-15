from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import date

def update_dataset():

    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cursor = conn.cursor()

    today_str = date.today().strftime("%Y-%m-%d")
    valeur_col2 = "310782347"
    projet_default = "devloppement osiris_rwd"

    sql = """
        INSERT INTO osiris_rwd.dataset (origincenterid, datasetupdatedate)
        VALUES (%s, %s, %s)
    """
    cursor.execute(sql, (valeur_col2, today_str,projet_default))
    conn.commit()
    cursor.close()
    conn.close()

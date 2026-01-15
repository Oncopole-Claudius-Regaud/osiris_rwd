from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import date
import pandas as pd


def load_lastnews():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    engine = hook.get_sqlalchemy_engine()

    # --------------------
    # Lecture source
    # --------------------
    query = """
        SELECT
            patientid,
            date_of_death,
            date_derniere_nouvelle
        FROM datamart_oeci_survie.v_date_derniere_nouvelle_combinee
    """
    df = pd.read_sql(query, engine)

    today = date.today()

    # --------------------
    # Colonnes fixes
    # --------------------
    df["ipp_ocr"] = df["patientid"]
    df["lastnewsid"] = ""

    # --------------------
    # Vital status
    # --------------------
    df["vitalstatus"] = df["date_of_death"].apply(
        lambda x: "vivant" if pd.isna(x) else "décédé"
    )

    df["vitalstatusupdateday"] = today.day
    df["vitalstatusupdatemonth"] = today.month
    df["vitalstatusupdateyear"] = today.year

    # --------------------
    # Death date (si existe)
    # --------------------
    df["deathdateday"] = df["date_of_death"].dt.day
    df["deathdatemonth"] = df["date_of_death"].dt.month
    df["deathdateyear"] = df["date_of_death"].dt.year

    # --------------------
    # Last visit date
    # --------------------
    df["lastvisitdateday"] = df["date_derniere_nouvelle"].dt.day
    df["lastvisitdatemonth"] = df["date_derniere_nouvelle"].dt.month
    df["lastvisitdateyear"] = df["date_derniere_nouvelle"].dt.year

    # --------------------
    # Colonnes finales
    # --------------------
    df_final = df[
        [
            "ipp_ocr",
            "lastnewsid",
            "vitalstatus",
            "vitalstatusupdateday",
            "vitalstatusupdatemonth",
            "vitalstatusupdateyear",
            "deathdateday",
            "deathdatemonth",
            "deathdateyear",
            "lastvisitdateday",
            "lastvisitdatemonth",
            "lastvisitdateyear",
        ]
    ]

    # --------------------
    # TRUNCATE + RELOAD
    # --------------------
    with engine.begin() as conn:
        conn.execute("TRUNCATE TABLE osiris_rwd.lastnews")

    df_final.to_sql(
        name="lastnews",
        con=engine,
        schema="osiris_rwd",
        if_exists="append",
        index=False
    )

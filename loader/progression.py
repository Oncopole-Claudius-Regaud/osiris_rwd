from __future__ import annotations

import json
import os
import shlex
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


REMOTE_HOST = "srvlakehouse"
REMOTE_PORT = 22
REMOTE_USER = "administrateur"
SSH_PASSWORD_VAR_KEY = "password_serverlakehouse"
REMOTE_BASE_DIR = "/opt/extract_osiris_rwd"
REMOTE_SCRIPT = f"{REMOTE_BASE_DIR}/scripts/extract_progression_by_ipp.py"
REMOTE_SOURCE_DIR = "/opt/PDF"
REMOTE_OUTPUT_DIR = f"{REMOTE_BASE_DIR}/output"
REMOTE_WORK_DIR = f"{REMOTE_BASE_DIR}/work/progression"
REMOTE_JSONL_NAME = "progression_results.jsonl"
LOCAL_RESULT_PATH = "/tmp/etl_iris/progression_results.jsonl"


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


def date_from_parts(day, month, year):
    try:
        if not year or not month:
            return None
        return date(int(year), int(month), int(day or 1))
    except (TypeError, ValueError):
        return None


def get_ssh_client():
    import paramiko

    password = Variable.get(SSH_PASSWORD_VAR_KEY)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=REMOTE_HOST,
        port=REMOTE_PORT,
        username=REMOTE_USER,
        password=password,
        timeout=30,
        allow_agent=False,
        look_for_keys=False,
    )
    return client


def get_patient_context(cur) -> list[dict[str, Optional[str]]]:
    cur.execute(
        """
        SELECT
            p.patientid,
            pc.cancerdiagnosisdateday,
            pc.cancerdiagnosisdatemonth,
            pc.cancerdiagnosisdateyear
        FROM osiris_rwd.patient p
        LEFT JOIN osiris_rwd.primarycancer pc
          ON pc.patientid = p.patientid
        ORDER BY p.patientid
        """
    )
    by_patient: dict[str, Optional[date]] = {}
    for patientid, day, month, year in cur.fetchall():
        patientid = str(patientid).strip()
        if not patientid:
            continue
        diagnosis_date = date_from_parts(day, month, year)
        current = by_patient.get(patientid)
        if diagnosis_date and (current is None or diagnosis_date < current):
            by_patient[patientid] = diagnosis_date
        else:
            by_patient.setdefault(patientid, current)
    return [
        {
            "patientid": patientid,
            "diagnosis_date": diagnosis_date.isoformat() if diagnosis_date else None,
        }
        for patientid, diagnosis_date in sorted(by_patient.items())
    ]


def remote_run_extract(patients: list[dict[str, Optional[str]]]) -> str:
    client = get_ssh_client()
    local_ipp_file: Optional[str] = None
    remote_ipp_file: Optional[str] = None
    sftp = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            prefix="osiris_rwd_progression_ipps_",
            delete=False,
            encoding="utf-8",
        ) as tmp:
            json.dump({"patients": patients}, tmp, ensure_ascii=False)
            local_ipp_file = tmp.name

        remote_ipp_file = f"{REMOTE_WORK_DIR}/{os.path.basename(local_ipp_file)}"
        remote_result_path = f"{REMOTE_OUTPUT_DIR}/{REMOTE_JSONL_NAME}"

        sftp = client.open_sftp()
        cmd_prepare = " ".join(
            [
                "mkdir",
                "-p",
                shlex.quote(REMOTE_OUTPUT_DIR),
                shlex.quote(REMOTE_WORK_DIR),
            ]
        )
        _, stdout, stderr = client.exec_command(cmd_prepare, get_pty=True)
        stdout.read()
        stderr_txt = stderr.read().decode("utf-8", errors="replace")
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            raise RuntimeError(f"Progression remote prepare failed with code {exit_status}: {stderr_txt[:2000]}")

        sftp.put(local_ipp_file, remote_ipp_file)

        cmd = " ".join(
            [
                "rm",
                "-f",
                shlex.quote(remote_result_path),
                "&&",
                "python3",
                shlex.quote(REMOTE_SCRIPT),
                "--source-dir",
                shlex.quote(REMOTE_SOURCE_DIR),
                "--ipp-file",
                shlex.quote(remote_ipp_file),
                "--output-dir",
                shlex.quote(REMOTE_OUTPUT_DIR),
                "--jsonl-name",
                shlex.quote(REMOTE_JSONL_NAME),
                "--log-level",
                "INFO",
            ]
        )

        _, stdout, stderr = client.exec_command(cmd, get_pty=True)
        stdout_txt = stdout.read().decode("utf-8", errors="replace")
        stderr_txt = stderr.read().decode("utf-8", errors="replace")
        exit_status = stdout.channel.recv_exit_status()
        if stdout_txt.strip():
            print("Progression remote stdout tail:")
            print("\n".join(stdout_txt.strip().splitlines()[-30:]))
        if stderr_txt.strip():
            print("Progression remote stderr tail:")
            print("\n".join(stderr_txt.strip().splitlines()[-30:]))
        if exit_status != 0:
            detail = (stderr_txt or stdout_txt).strip()[:2000]
            raise RuntimeError(f"Progression remote extraction failed with code {exit_status}: {detail}")

        Path(LOCAL_RESULT_PATH).parent.mkdir(parents=True, exist_ok=True)
        sftp.get(remote_result_path, LOCAL_RESULT_PATH)
        return LOCAL_RESULT_PATH
    finally:
        if sftp is not None:
            if remote_ipp_file:
                try:
                    sftp.remove(remote_ipp_file)
                except Exception:
                    pass
            sftp.close()
        if local_ipp_file and os.path.exists(local_ipp_file):
            os.unlink(local_ipp_file)
        client.close()


def insert_progression(cur, row: dict) -> None:
    patientid = (row.get("patientid") or "").strip()
    progressionsource = (row.get("progressionsource") or "").strip() or None
    day = row.get("progressiondateday")
    month = row.get("progressiondatemonth")
    year = row.get("progressiondateyear")

    if not patientid or not progressionsource:
        return

    cur.execute(
        """
        INSERT INTO osiris_rwd.progression (
            patientid,
            progressiondateday,
            progressiondatemonth,
            progressiondateyear,
            progressionsource
        )
        SELECT %s, %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1
            FROM osiris_rwd.progression
            WHERE patientid = %s
              AND progressiondateday IS NOT DISTINCT FROM %s
              AND progressiondatemonth IS NOT DISTINCT FROM %s
              AND progressiondateyear IS NOT DISTINCT FROM %s
              AND progressionsource IS NOT DISTINCT FROM %s
        )
        """,
        (
            patientid,
            day,
            month,
            year,
            progressionsource,
            patientid,
            day,
            month,
            year,
            progressionsource,
        ),
    )


def load_progression():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        patients = get_patient_context(cur)
        if not patients:
            return

        result_path = remote_run_extract(patients)
        cur.execute("TRUNCATE TABLE osiris_rwd.progression RESTART IDENTITY")

        loaded = 0
        with open(result_path, "r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                insert_progression(cur, json.loads(line))
                loaded += 1

        conn.commit()
        print(
            "Progression rows processed: "
            f"{loaded}; table truncated before load at {datetime.utcnow().isoformat()}"
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

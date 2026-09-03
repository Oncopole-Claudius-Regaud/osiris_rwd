from __future__ import annotations

import json
import os
import shlex
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


REMOTE_HOST = "srvlakehouse"
REMOTE_PORT = 22
REMOTE_USER = "administrateur"
SSH_PASSWORD_VAR_KEY = "password_serverlakehouse"
REMOTE_BASE_DIR = "/opt/extract_osiris_rwd"
REMOTE_SCRIPT = f"{REMOTE_BASE_DIR}/scripts/extract_familycancerhistory_by_ipp.py"
REMOTE_SOURCE_DIR = "/opt/PDF"
REMOTE_OUTPUT_DIR = f"{REMOTE_BASE_DIR}/output"
REMOTE_JSONL_NAME = "familycancerhistory_results.jsonl"
LOCAL_RESULT_PATH = "/tmp/etl_iris/familycancerhistory_results.jsonl"


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


def get_loaded_patientids(cur) -> list[str]:
    cur.execute("SELECT patientid FROM osiris_rwd.patient ORDER BY patientid")
    return [str(row[0]).strip() for row in cur.fetchall() if row[0]]


def get_table_columns(cur) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'osiris_rwd'
          AND table_name = 'familycancerhistory'
        """
    )
    return {row[0].lower() for row in cur.fetchall()}


def remote_run_extract(patientids: list[str]) -> str:
    client = get_ssh_client()
    local_ipp_file: Optional[str] = None
    remote_ipp_file: Optional[str] = None
    sftp = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            prefix="osiris_rwd_familycancerhistory_ipps_",
            delete=False,
            encoding="utf-8",
        ) as tmp:
            json.dump({"ipp_list": patientids}, tmp, ensure_ascii=False)
            local_ipp_file = tmp.name

        remote_ipp_file = f"/tmp/{os.path.basename(local_ipp_file)}"
        remote_result_path = f"{REMOTE_OUTPUT_DIR}/{REMOTE_JSONL_NAME}"

        sftp = client.open_sftp()
        sftp.put(local_ipp_file, remote_ipp_file)

        cmd = " ".join(
            [
                "mkdir",
                "-p",
                shlex.quote(REMOTE_OUTPUT_DIR),
                "&&",
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
            print("FamilyCancerHistory remote stdout tail:")
            print("\n".join(stdout_txt.strip().splitlines()[-30:]))
        if stderr_txt.strip():
            print("FamilyCancerHistory remote stderr tail:")
            print("\n".join(stderr_txt.strip().splitlines()[-30:]))
        if exit_status != 0:
            detail = (stderr_txt or stdout_txt).strip()[:2000]
            raise RuntimeError(f"FamilyCancerHistory remote extraction failed with code {exit_status}: {detail}")

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


def truncate_familycancerhistory(cur) -> int:
    cur.execute("TRUNCATE TABLE osiris_rwd.familycancerhistory RESTART IDENTITY")
    return cur.rowcount


PARENTAGE_SNOMED = {
    "Mother": "72705000",
    "Father": "66839005",
    "Son": "65616008",
    "Daughter": "66089001",
    "Grandfather": "34871008",
    "Grandmother": "113157001",
    "Grandson": "70578009",
    "Granddaughter": "44181008",
    "Brother": "70924004",
    "Sister": "27733009",
}


def map_parentage(value: str) -> str | None:
    value = (value or "").strip()
    if not value:
        return None
    if value.isdigit():
        return value
    mapped = PARENTAGE_SNOMED.get(value)
    if mapped:
        return mapped
    return None


def insert_familycancerhistory(cur, row: dict) -> int:
    patientid = (row.get("patientid") or "").strip()
    topo = (row.get("familycancertopocode") or "").strip()
    parentage = map_parentage(row.get("familycancerparentage") or "")
    if not patientid or not topo or not parentage:
        return 0

    cur.execute(
        """
        INSERT INTO osiris_rwd.familycancerhistory (
            patientid,
            familycancertopocode,
            familycancerparentage
        )
        SELECT %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1
            FROM osiris_rwd.familycancerhistory
            WHERE patientid = %s
              AND familycancertopocode = %s
              AND familycancerparentage = %s
        )
        """,
        (patientid, topo, parentage, patientid, topo, parentage),
    )
    return cur.rowcount


def load_familycancerhistory():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        patientids = get_loaded_patientids(cur)
        if not patientids:
            return

        result_path = remote_run_extract(patientids)
        columns = get_table_columns(cur)
        if not columns:
            raise RuntimeError("Table osiris_rwd.familycancerhistory introuvable")

        truncate_familycancerhistory(cur)
        processed = 0
        inserted = 0
        with open(result_path, "r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                inserted += insert_familycancerhistory(cur, json.loads(line))
                processed += 1

        conn.commit()
        print(
            "FamilyCancerHistory rows inserted: "
            f"{inserted}; rows processed: {processed}; table truncated before load at {datetime.utcnow().isoformat()}"
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

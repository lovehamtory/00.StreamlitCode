from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

try:
    import psycopg
except ImportError:
    psycopg = None


PROJECT_ROOT = Path(__file__).parent.parent
SCHEMA_CONFIG = PROJECT_ROOT / "app" / ".streamlit" / "migration_setup.toml"
REQUIRED_TABLES = {"tb_mig_usr", "tb_mig_auth_grp", "tb_mig_menu_auth", "tb_mig_usr_auth", "tb_mig_conn", "tb_mig_airflow", "tb_mig_emr", "tb_mig_sbj_area", "tb_mig_sbj_dag_mpg", "tb_mig_dag_dply_hist", "tb_mig_emr_run", "tb_mig_src_layout", "tb_mig_tbl_mpg", "tb_mig_col_mpg", "tb_mig_mpg_chg_hist", "tb_mig_s3_manf", "tb_mig_dag_run", "tb_mig_run_log", "tb_mig_vald_rslt", "tb_mig_vald_col_rslt", "tb_mig_tbl_load_hist", "tb_mig_artf_item"}


def text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def schema_name(value: object) -> str:
    candidate = text(value).lower()
    if not re.fullmatch(r"[a-z_][a-z0-9_]{0,126}", candidate):
        raise ValueError("스키마명은 영문 소문자·숫자·밑줄 1~127자리여야 합니다.")
    return candidate


def saved_schema() -> str:
    if not SCHEMA_CONFIG.exists():
        return ""
    try:
        with SCHEMA_CONFIG.open("rb") as source:
            return schema_name(tomllib.load(source).get("migration", {}).get("schema", ""))
    except (OSError, tomllib.TOMLDecodeError, ValueError):
        return ""


def configured_schema() -> str:
    return saved_schema()


def connection_values() -> dict[str, Any]:
    try:
        settings = dict(st.secrets.get("migration_metadata", {}))
        secret_sections = st.secrets
    except Exception:
        settings = {}
        secret_sections = {}
    section = text(settings.get("connection_section")) or "tgt_red"
    if section not in secret_sections:
        raise ValueError("초기 설정용 Redshift 연결 설정이 없습니다.")
    values = dict(secret_sections[section])
    required = ("host", "port", "database", "user", "password")
    if [key for key in required if not text(values.get(key))]:
        raise ValueError("초기 설정용 Redshift 연결 필수 항목이 없습니다.")
    return values


def connect(values: dict[str, Any]) -> Any:
    if psycopg is None:
        raise RuntimeError(f"psycopg가 현재 실행 Python에 설치되지 않았습니다: {sys.executable}")
    arguments: dict[str, Any] = {"host": text(values["host"]), "port": int(values["port"]), "dbname": text(values["database"]), "user": text(values["user"]), "password": text(values["password"]), "connect_timeout": int(values.get("connect_timeout", 15))}
    if text(values.get("sslmode")):
        arguments["sslmode"] = text(values["sslmode"])
    return psycopg.connect(**arguments)


def metadata_ready(values: dict[str, Any], schema: str) -> bool:
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = ANY(%s)", (schema, list(REQUIRED_TABLES)))
            return {text(row[0]).lower() for row in cursor.fetchall()} == REQUIRED_TABLES


def ddl_source(schema: str) -> str:
    source = (PROJECT_ROOT / "sql" / "01_mig_metadata_ddl.sql").read_text(encoding="utf-8")
    converted = source.replace("MIG_META", schema.upper())
    if "MIG_META" in converted:
        raise ValueError("메타데이터 스키마 치환을 완료할 수 없습니다.")
    return converted


def save_schema(schema: str) -> None:
    SCHEMA_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    SCHEMA_CONFIG.write_text(f"[migration]\nschema = \"{schema}\"\n", encoding="utf-8")


def initialize(values: dict[str, Any], schema: str) -> None:
    item_schema = schema_name(schema)
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = ANY(%s) LIMIT 1", (item_schema, list(REQUIRED_TABLES)))
            if cursor.fetchone() is not None:
                raise ValueError("기존 이관 메타가 있어 최초 메타 설치를 실행할 수 없습니다.")
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{item_schema}"')
            cursor.execute(ddl_source(item_schema))
        connection.commit()
    save_schema(item_schema)


def render_initial_setup() -> None:
    st.info("최초 이관 메타를 설치합니다. 기존 이관 메타가 있으면 설치하지 않습니다.", icon=":material/info:")
    default_schema = configured_schema()
    value = st.text_input("메타데이터 스키마", value=default_schema, placeholder="예: migration_meta")
    submitted = st.button("메타 설치", type="primary", icon=":material/play_circle:", width="content")
    if submitted:
        try:
            from SrcTgtSecurity import require_save
            require_save("INIT")
            initialize(connection_values(), schema_name(value))
            st.success("이관 메타를 설치했습니다. 다시 로그인하십시오.", icon=":material/check_circle:")
            st.rerun()
        except Exception as error:
            st.error(f"메타 설치 실패: {error}", icon=":material/error:")

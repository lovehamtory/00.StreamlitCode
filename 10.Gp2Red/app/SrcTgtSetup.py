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
SCHEMA_CONFIG = PROJECT_ROOT.parent / ".streamlit" / "migration_setup.toml"
REQUIRED_TABLES = {"tb_mig_sbj_area", "tb_mig_sbj_dag_mpg", "tb_mig_usr", "tb_mig_usr_auth", "tb_mig_tbl_mpg", "tb_mig_col_mpg", "tb_mig_run_log", "tb_mig_artf_item"}


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
    settings = dict(st.secrets.get("migration_metadata", {}))
    return text(settings.get("schema")).lower() or saved_schema()


def connection_values() -> dict[str, Any]:
    settings = dict(st.secrets.get("migration_metadata", {}))
    section = text(settings.get("connection_section")) or "redshift_sql"
    if section not in st.secrets:
        raise ValueError("초기 설정용 Redshift 연결 설정이 없습니다.")
    values = dict(st.secrets[section])
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
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (schema,))
            if cursor.fetchone() is None:
                raise ValueError("선택한 스키마가 없습니다. DBA가 스키마를 생성한 뒤 다시 실행하십시오.")
            cursor.execute(ddl_source(schema))
        connection.commit()
    save_schema(schema)


def backup_table_names(schema: str, standard_date: str) -> list[tuple[str, str]]:
    return [(table_name, f"{table_name}_{standard_date}") for table_name in sorted(REQUIRED_TABLES)]


def backup_metadata(values: dict[str, Any], schema: str, standard_date: str) -> int:
    backups = backup_table_names(schema, standard_date)
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = ANY(%s)", (schema, [source for source, _ in backups]))
            existing = {text(row[0]).lower() for row in cursor.fetchall()}
            if not existing:
                raise ValueError("백업할 이관 메타 테이블이 없습니다.")
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = ANY(%s)", (schema, [target for _, target in backups]))
            duplicate = {text(row[0]).lower() for row in cursor.fetchall()}
            if duplicate:
                raise ValueError(f"오늘자 백업 테이블이 이미 있습니다: {', '.join(sorted(duplicate))}")
            for source, target in backups:
                if source in existing:
                    cursor.execute(f"CREATE TABLE {schema}.{target} AS SELECT * FROM {schema}.{source}")
        connection.commit()
    return len(existing)


def render_initial_setup() -> None:
    st.title("⚙️ 이관 초기 설정")
    st.caption("⚙️ Created by ♡홍율파파♡")
    st.warning("DBA가 만든 스키마를 선택하십시오. 메타 생성은 그 안의 이관 메타 뷰·테이블만 삭제한 뒤 다시 만듭니다.", icon=":material/warning:")
    default_schema = configured_schema()
    value = st.text_input("메타데이터 스키마", value=default_schema, placeholder="예: migration_meta")
    backup, create = st.columns(2)
    with backup:
        backed_up = st.button("메타데이터 백업", icon=":material/backup:", width="stretch")
    with create:
        submitted = st.button("메타 생성", type="primary", icon=":material/play_circle:", width="stretch")
    if backed_up:
        try:
            count = backup_metadata(connection_values(), schema_name(value), pd.Timestamp.now().strftime("%Y%m%d"))
            st.success(f"메타 테이블 {count:,}건을 오늘자 CTAS 백업으로 생성했습니다.", icon=":material/check_circle:")
        except Exception as error:
            st.error(f"메타데이터 백업 실패: {error}", icon=":material/error:")
    if submitted:
        try:
            initialize(connection_values(), schema_name(value))
            st.success("메타데이터를 생성했습니다. 초기 관리자 로그인으로 계속하십시오.", icon=":material/check_circle:")
            st.rerun()
        except Exception as error:
            st.error(f"초기 설정 실패: {error}", icon=":material/error:")

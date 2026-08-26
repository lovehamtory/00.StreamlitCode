from __future__ import annotations

from dataclasses import dataclass
import sys
from typing import Any

import pandas as pd
import streamlit as st

from SrcTgtSetup import configured_schema, connection_values

try:
    import psycopg
except ImportError:
    psycopg = None


@dataclass(frozen=True)
class RuntimeContext:
    values: dict[str, Any]
    schema_name: str


def text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def identifier(value: object) -> str:
    name = text(value)
    if not name or "\x00" in name or len(name.encode("utf-8")) > 127:
        raise ValueError("메타데이터 식별자 형식이 올바르지 않습니다.")
    return '"' + name.replace('"', '""') + '"'


def qualified(schema_name: str, table_name: str) -> str:
    return f"{identifier(schema_name)}.{identifier(table_name)}"


def connect(values: dict[str, Any]) -> Any:
    if psycopg is None:
        raise RuntimeError(f"psycopg가 현재 실행 Python에 설치되지 않았습니다: {sys.executable}")
    arguments: dict[str, Any] = {
        "host": text(values["host"]), "port": int(values["port"]), "dbname": text(values["database"]),
        "user": text(values["user"]), "password": text(values["password"]),
        "connect_timeout": int(values.get("connect_timeout", 15)),
    }
    if text(values.get("sslmode")):
        arguments["sslmode"] = text(values["sslmode"])
    return psycopg.connect(**arguments)


def query_frame(values: dict[str, Any], query: str, parameters: tuple[Any, ...] = ()) -> pd.DataFrame:
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
            return pd.DataFrame(cursor.fetchall(), columns=[column.name for column in cursor.description])


def runtime_context() -> RuntimeContext:
    schema_name = configured_schema()
    if not schema_name:
        raise ValueError("초기 설정에서 메타데이터 스키마를 먼저 생성하십시오.")
    return RuntimeContext(values=connection_values(), schema_name=schema_name)


def public_monitor_context() -> RuntimeContext:
    settings = dict(st.secrets.get("migration_monitor", {}))
    section = text(settings.get("connection_section"))
    if not section:
        return runtime_context()
    schema_name = text(settings.get("schema")).lower() or "mig_meta"
    if section not in st.secrets:
        raise ValueError("고객 현황 연결 설정이 없습니다.")
    values = dict(st.secrets[section])
    required = ("host", "port", "database", "user", "password")
    if [key for key in required if not text(values.get(key))]:
        raise ValueError("고객 현황 연결 필수 항목이 없습니다.")
    return RuntimeContext(values=values, schema_name=schema_name)

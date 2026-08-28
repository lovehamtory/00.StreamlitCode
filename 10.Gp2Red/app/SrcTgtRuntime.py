from __future__ import annotations

from dataclasses import dataclass
import re
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


WRITE_SQL_PATTERN = re.compile(r"^\s*(?:INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|COPY|GRANT|REVOKE|COMMENT)\b", re.IGNORECASE)


def ensure_write_allowed(statement: object) -> None:
    if not WRITE_SQL_PATTERN.match(str(statement)):
        return
    user = st.session_state.get("mig_authenticated_user")
    menu_code = text(st.session_state.get("mig_active_menu"))
    if not isinstance(user, dict) or not menu_code:
        return
    permissions = user.get("permissions", {}).get(menu_code, {})
    if not bool(permissions.get("save", False)):
        raise PermissionError("이 메뉴의 저장 권한이 없습니다.")


class GuardedCursor:
    def __init__(self, cursor: Any) -> None:
        self.cursor = cursor

    def __enter__(self) -> GuardedCursor:
        self.cursor.__enter__()
        return self

    def __exit__(self, *args: object) -> object:
        return self.cursor.__exit__(*args)

    def execute(self, statement: object, *args: object, **kwargs: object) -> Any:
        ensure_write_allowed(statement)
        return self.cursor.execute(statement, *args, **kwargs)

    def executemany(self, statement: object, *args: object, **kwargs: object) -> Any:
        ensure_write_allowed(statement)
        return self.cursor.executemany(statement, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.cursor, name)


class GuardedConnection:
    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def __enter__(self) -> GuardedConnection:
        self.connection.__enter__()
        return self

    def __exit__(self, *args: object) -> object:
        return self.connection.__exit__(*args)

    def cursor(self, *args: object, **kwargs: object) -> GuardedCursor:
        return GuardedCursor(self.connection.cursor(*args, **kwargs))

    def __getattr__(self, name: str) -> Any:
        return getattr(self.connection, name)


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
    return GuardedConnection(psycopg.connect(**arguments))


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

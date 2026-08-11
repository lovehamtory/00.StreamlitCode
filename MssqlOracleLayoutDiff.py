from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Any

import oracledb
import pandas as pd
import streamlit as st


DEFAULT_LAYOUT_TABLE = "TB_TABLE_LAYOUT_TLP"
DEFAULT_TARGET_OWNER = "PCERP_RENTALAPP_MIG"
DEFAULT_TABLESPACE = "PCERP_MIG_DATA"
DEFAULT_INDEX_TABLESPACE = "PCERP_MIG_INEX"
DDL_PREVIEW_MAX_LINES = 500
ORACLE_RESERVED_WORDS = {
    "ACCESS", "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUDIT", "BETWEEN",
    "BY", "CHAR", "CHECK", "CLUSTER", "COLUMN", "COMMENT", "COMPRESS", "CONNECT", "CREATE",
    "CURRENT", "DATE", "DECIMAL", "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "ELSE",
    "EXCLUSIVE", "EXISTS", "FILE", "FLOAT", "FOR", "FROM", "GRANT", "GROUP", "HAVING", "IDENTIFIED",
    "IMMEDIATE", "IN", "INCREMENT", "INDEX", "INITIAL", "INSERT", "INTEGER", "INTERSECT", "INTO",
    "IS", "LEVEL", "LIKE", "LOCK", "LONG", "MAXEXTENTS", "MINUS", "MLSLABEL", "MODE", "MODIFY",
    "NOAUDIT", "NOCOMPRESS", "NOT", "NOWAIT", "NULL", "NUMBER", "OF", "OFFLINE", "ON", "ONLINE",
    "OPTION", "OR", "ORDER", "PCTFREE", "PRIOR", "PRIVILEGES", "PUBLIC", "RAW", "RENAME", "RESOURCE",
    "REVOKE", "ROW", "ROWID", "ROWLABEL", "ROWNUM", "ROWS", "SELECT", "SESSION", "SET", "SHARE",
    "SIZE", "SMALLINT", "START", "SUCCESSFUL", "SYNONYM", "SYSDATE", "TABLE", "THEN", "TO", "TRIGGER",
    "UID", "UNION", "UNIQUE", "UPDATE", "USER", "VALIDATE", "VALUES", "VARCHAR", "VARCHAR2", "VIEW",
    "WHENEVER", "WHERE", "WITH",
}


@dataclass(frozen=True)
class DdlStatement:
    action: str
    source_db: str
    source_table: str
    target_table: str
    sql: str


@dataclass(frozen=True)
class GeneratedDdl:
    statements: tuple[DdlStatement, ...]
    text: str
    structure_text: str
    comment_text: str
    generated_at: str
    table_count: int
    comment_count: int
    source_keys: tuple[tuple[str, str], ...]


def init_state() -> None:
    defaults: dict[str, Any] = {
        "comparison": None,
        "ddl_artifact": None,
        "ddl_logs": None,
        "selected_owner": None,
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)


def sql_name(value: str) -> str:
    name = str(value or "").strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_$#]{0,127}", name):
        raise ValueError(f"Oracle 객체명으로 사용할 수 없습니다: {value}")
    return name


def clean_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def normalized(value: object) -> str:
    return re.sub(r"\s+", " ", clean_text(value)).upper()


def normalized_series(values: pd.Series) -> pd.Series:
    return values.astype("string").fillna("").str.strip().str.replace(r"\s+", " ", regex=True).str.upper()


def comparison_key(value: object) -> str:
    text = clean_text(value)
    if len(text) >= 2 and ((text.startswith('"') and text.endswith('"')) or (text.startswith("[") and text.endswith("]"))):
        text = text[1:-1]
    return re.sub(r"\s+", " ", text).upper()


def snapshot_label(value: object) -> str:
    raw = clean_text(value)
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:]}"
    return raw


def colno_number(value: object) -> int:
    try:
        return int(float(clean_text(value)))
    except (TypeError, ValueError):
        return 999999999


def parse_length(value: object) -> tuple[int | None, int | None, bool]:
    raw = clean_text(value).replace(" ", "")
    if not raw or raw.upper() == "MAX":
        return None, None, raw.upper() == "MAX"
    match = re.fullmatch(r"(\d+)(?:,(\d+))?", raw)
    if not match:
        return None, None, False
    return int(match.group(1)), int(match.group(2)) if match.group(2) else None, False


def q_literal(value: object) -> str:
    text = re.sub(r"[\r\n]+", " ", clean_text(value)).strip()[:3900]
    for opening, closing in (("[", "]"), ("{", "}"), ("(", ")"), ("<", ">")):
        if closing not in text:
            return f"q'{opening}{text}{closing}'"
    return "'" + text.replace("'", "''") + "'"


def source_column_name(value: object, colno: object, used: set[str]) -> str:
    raw = re.sub(r"\s+", "", clean_text(value).strip("'").upper())
    cleaned = re.sub(r"[^A-Za-z0-9_$\#ㄱ-ㅎㅏ-ㅣ가-힣]", "_", raw)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    requires_prefix = not raw or not raw[0].isalpha()
    base = raw if not requires_prefix else f"C{cleaned}"
    base = base[:30]
    candidate = base
    suffix = f"_{colno_number(colno)}"
    if candidate in used:
        candidate = f"{base[:30 - len(suffix)]}{suffix}"
    sequence = 2
    while candidate in used:
        numbered = f"_{sequence}"
        candidate = f"{base[:30 - len(suffix) - len(numbered)]}{suffix}{numbered}"
        sequence += 1
    used.add(candidate)
    return candidate


def oracle_column_identifier(value: str) -> str:
    return f'"{value}"' if value in ORACLE_RESERVED_WORDS else value


def fallback_target_table(owner: str, table: str) -> str:
    source = f"{normalized(owner)}_{normalized(table)}"
    source = re.sub(r"[^A-Za-z0-9_$\#ㄱ-ㅎㅏ-ㅣ가-힣]", "_", source)
    source = re.sub(r"_+", "_", source).strip("_")
    if not source or not source[0].isalpha():
        source = f"T_{source}"
    return source[:128]


def target_table_name(owner: str, table: str) -> str:
    return fallback_target_table(owner, table)


def is_not_null(value: object) -> bool:
    return normalized(value) in {"N", "NO", "NOT NULL", "NOT_NULL", "0", "FALSE"}


def is_primary_key(value: object) -> bool:
    return normalized(value) in {"Y", "YES", "PK", "P", "1", "TRUE"}


def mssql_to_oracle(data_type: object, length: object, character_multiplier: int) -> str:
    data_type_text = normalized(data_type).lower()
    precision, scale, is_max = parse_length(length)
    char_types = {"varchar", "varchar2", "nvarchar", "nvarchar2", "char", "nchar"}
    if data_type_text in char_types:
        if is_max or precision is None or precision <= 0:
            return "CLOB"
        converted_length = math.ceil(precision * character_multiplier)
        return "CLOB" if converted_length > 4000 else f"VARCHAR2({converted_length})"
    if data_type_text in {"text", "ntext", "xml", "sql_variant"}:
        return "CLOB"
    if data_type_text in {"tinyint", "smallint", "int", "integer", "bigint"}:
        digits = {"tinyint": 3, "smallint": 5, "int": 10, "integer": 10, "bigint": 19}[data_type_text]
        return f"NUMBER({digits})"
    if data_type_text in {"bit", "boolean"}:
        return "NUMBER(1)"
    if data_type_text in {"decimal", "numeric", "number"}:
        if precision is not None and scale is not None:
            return f"NUMBER({precision},{scale})"
        if precision is not None:
            return f"NUMBER({precision})"
        return "NUMBER"
    if data_type_text in {"money", "smallmoney"}:
        return "NUMBER(19,4)"
    if data_type_text in {"float", "real"}:
        return "NUMBER(38,17)"
    if data_type_text in {"datetime", "datetime2", "smalldatetime", "date"}:
        return "TIMESTAMP"
    if data_type_text in {"datetimeoffset"}:
        return "TIMESTAMP WITH TIME ZONE"
    if data_type_text in {"time"}:
        return "VARCHAR2(30)"
    if data_type_text in {"binary", "varbinary", "image", "rowversion", "timestamp"}:
        return "BLOB"
    if data_type_text in {"uniqueidentifier", "guid"}:
        return "VARCHAR2(36)"
    return "VARCHAR2(4000)"


@st.cache_resource(show_spinner=False)
def get_pool() -> oracledb.ConnectionPool:
    conf = st.secrets["oracle"]
    dsn = f"{conf['host']}:{conf['port']}/{conf['service_name']}"
    return oracledb.create_pool(
        user=conf["user"],
        password=conf["password"],
        dsn=dsn,
        min=1,
        max=4,
        increment=1,
    )


def dataframe_from_cursor(cursor: oracledb.Cursor) -> pd.DataFrame:
    columns = [description[0].upper() for description in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


@st.cache_data(ttl=120, show_spinner=False)
def fetch_snapshot_dates(layout_owner: str, layout_table: str) -> list[str]:
    owner = sql_name(layout_owner)
    table = sql_name(layout_table)
    with get_pool().acquire() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT DISTINCT STD_DT FROM {owner}.{table} ORDER BY STD_DT")
            return [clean_text(row[0]) for row in cursor.fetchall() if clean_text(row[0])]


@st.cache_data(ttl=120, max_entries=20, show_spinner=False)
def fetch_layout_pair(
    layout_owner: str,
    layout_table: str,
    before_snapshot: str,
    after_snapshot: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    owner = sql_name(layout_owner)
    table = sql_name(layout_table)
    query = f"""
        SELECT STD_DT, OWNER, TBL, ENTITY, COLNO, COL, ATTR, DATATYPE, LEN, ISPK, NULLABLE
          FROM {owner}.{table}
         WHERE STD_DT IN (:before_snapshot, :after_snapshot)
         ORDER BY OWNER, TBL, COLNO
    """
    with get_pool().acquire() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, {"before_snapshot": before_snapshot, "after_snapshot": after_snapshot})
            frame = dataframe_from_cursor(cursor)
    return (
        frame[frame["STD_DT"].map(clean_text) == before_snapshot].copy(),
        frame[frame["STD_DT"].map(clean_text) == after_snapshot].copy(),
    )




def table_signature(table_frame: pd.DataFrame) -> tuple[tuple[str, ...], ...]:
    signature_columns = ["_SIG_COLNO", "_SIG_COL", "_SIG_ATTR", "_SIG_DATATYPE", "_SIG_LEN", "_SIG_ISPK", "_SIG_NULLABLE"]
    ordered = table_frame.sort_values("_COLNO_ORDER")
    return tuple(ordered[signature_columns].itertuples(index=False, name=None))


def table_groups(frame: pd.DataFrame) -> dict[tuple[str, str], pd.DataFrame]:
    prepared = frame.copy()
    prepared["_COMPARE_OWNER"] = prepared["OWNER"].map(comparison_key)
    prepared["_COMPARE_TABLE"] = prepared["TBL"].map(comparison_key)
    prepared["_COLNO_ORDER"] = pd.to_numeric(prepared["COLNO"], errors="coerce").fillna(999999999)
    for column in ("COLNO", "COL", "ATTR", "DATATYPE", "LEN", "ISPK", "NULLABLE"):
        prepared[f"_SIG_{column}"] = normalized_series(prepared[column])
    groups: dict[tuple[str, str], pd.DataFrame] = {}
    for (owner, table), group in prepared.groupby(["_COMPARE_OWNER", "_COMPARE_TABLE"], dropna=False, sort=False):
        groups[(owner, table)] = group.copy()
    return groups


def first_entity(frame: pd.DataFrame) -> str:
    if frame.empty:
        return ""
    values = [clean_text(value) for value in frame["ENTITY"].tolist() if clean_text(value)]
    return values[0] if values else ""


def compare_layouts(before: pd.DataFrame, after: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    before_groups = table_groups(before)
    after_groups = table_groups(after)
    table_rows: list[dict[str, str]] = []
    column_rows: list[dict[str, str]] = []
    for owner, table in sorted(set(before_groups) | set(after_groups)):
        before_frame = before_groups.get((owner, table), pd.DataFrame(columns=before.columns))
        after_frame = after_groups.get((owner, table), pd.DataFrame(columns=after.columns))
        if before_frame.empty:
            status = "신규"
        elif after_frame.empty:
            status = "삭제"
        elif table_signature(before_frame) != table_signature(after_frame) or first_entity(before_frame) != first_entity(after_frame):
            status = "변경"
        else:
            continue
        before_entity = first_entity(before_frame)
        after_entity = first_entity(after_frame)
        entity = after_entity or before_entity
        table_rows.append(
            {
                "DB": owner,
                "테이블": table,
                "엔티티(전)": before_entity,
                "엔티티(후)": after_entity,
                "구분": status,
            }
        )
        before_by_colno = {normalized(row["COLNO"]): row for _, row in before_frame.iterrows()}
        after_by_colno = {normalized(row["COLNO"]): row for _, row in after_frame.iterrows()}
        for colno in sorted(set(before_by_colno) | set(after_by_colno), key=colno_number):
            previous = before_by_colno.get(colno)
            current = after_by_colno.get(colno)
            field_names = ("COL", "ATTR", "DATATYPE", "LEN", "ISPK", "NULLABLE")
            if previous is not None and current is not None and all(normalized(previous[name]) == normalized(current[name]) for name in field_names):
                continue
            column_status = "신규" if previous is None else "삭제" if current is None else "변경"
            column_rows.append(
                {
                    "DB": owner,
                    "테이블": table,
                    "엔티티": entity,
                    "구분": column_status,
                    "컬럼순서": colno,
                    "컬럼명(전)": clean_text(previous["COL"]) if previous is not None else "",
                    "컬럼명(후)": clean_text(current["COL"]) if current is not None else "",
                    "속성(전)": clean_text(previous["ATTR"]) if previous is not None else "",
                    "속성(후)": clean_text(current["ATTR"]) if current is not None else "",
                    "타입(전)": clean_text(previous["DATATYPE"]) if previous is not None else "",
                    "타입(후)": clean_text(current["DATATYPE"]) if current is not None else "",
                    "길이(전)": clean_text(previous["LEN"]) if previous is not None else "",
                    "길이(후)": clean_text(current["LEN"]) if current is not None else "",
                    "PK(전)": clean_text(previous["ISPK"]) if previous is not None else "",
                    "PK(후)": clean_text(current["ISPK"]) if current is not None else "",
                    "NOT NULL(전)": "Y" if previous is not None and is_not_null(previous["NULLABLE"]) else "",
                    "NOT NULL(후)": "Y" if current is not None and is_not_null(current["NULLABLE"]) else "",
                }
            )
    return pd.DataFrame(table_rows), pd.DataFrame(column_rows)


def storage_clause(size_gb: float | None) -> str:
    if size_gb is None or size_gb <= 0:
        return ""
    initial_gb = max(1, math.ceil(size_gb))
    next_gb = max(1, min(initial_gb, 16))
    return f" STORAGE (INITIAL {initial_gb}G NEXT {next_gb}G)"


def clean_metadata_ddl(text: str) -> str:
    value = text.read() if hasattr(text, "read") else str(text)
    value = re.sub(r"\s+", " ", value).strip()
    return value[:-1].strip() if value.endswith(";") else value


def fetch_current_table_sizes(target_owner: str, target_tables: list[str]) -> dict[str, float]:
    if not target_tables:
        return {}
    result: dict[str, float] = {}
    for start in range(0, len(target_tables), 900):
        current_tables = target_tables[start : start + 900]
        bind_names = [f"table_{index}" for index in range(len(current_tables))]
        bind_sql = ", ".join(f":{name}" for name in bind_names)
        binds: dict[str, str] = dict(zip(bind_names, current_tables))
        query = f"""
        SELECT TABLE_NAME, ROUND(SUM(BYTES) / 1024 / 1024 / 1024, 6) AS SIZE_GB
          FROM (
                SELECT NVL(L_SEG.TABLE_NAME, NVL(L_IDX.TABLE_NAME,
                       S.SEGMENT_NAME)) AS TABLE_NAME,
                       S.BYTES
                  FROM USER_SEGMENTS S
                  LEFT JOIN USER_LOBS L_SEG
                    ON L_SEG.SEGMENT_NAME = S.SEGMENT_NAME
                  LEFT JOIN USER_LOBS L_IDX
                    ON L_IDX.INDEX_NAME = S.SEGMENT_NAME
                 WHERE S.SEGMENT_TYPE IN (
                       'TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION',
                       'LOBSEGMENT', 'LOB PARTITION', 'LOB SUBPARTITION', 'LOBINDEX'
                   )
          )
         WHERE TABLE_NAME IN ({bind_sql})
         GROUP BY TABLE_NAME
    """
        try:
            with get_pool().acquire() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, binds)
                    result.update({normalized(row[0]): float(row[1]) for row in cursor.fetchall() if row[1] is not None})
        except Exception as exc:
            raise RuntimeError(
                f"Oracle 현재 세그먼트 크기 조회 실패: USER_SEGMENTS 또는 USER_LOBS 조회 오류입니다. 원본 오류: {exc}"
            ) from exc
    return result


def existing_index_statements(cursor: oracledb.Cursor, target_owner: str, target_table: str) -> list[str]:
    query = """
        SELECT i.index_name
          FROM user_indexes i
         WHERE i.table_name = :table_name
           AND i.index_type NOT IN ('LOB', 'IOT - TOP')
           AND i.index_name NOT LIKE 'SYS$_%' ESCAPE '$'
           AND NOT EXISTS (
                SELECT 1
                  FROM user_constraints c
                 WHERE c.table_name = i.table_name
                   AND c.constraint_type IN ('P', 'U')
                   AND c.index_name = i.index_name
           )
         ORDER BY i.index_name
    """
    cursor.execute(query, {"table_name": target_table})
    statements: list[str] = []
    for (index_name,) in cursor.fetchall():
        cursor.execute(
            "SELECT DBMS_METADATA.GET_DDL('INDEX', :index_name, :index_owner) FROM DUAL",
            {"index_name": index_name, "index_owner": target_owner},
        )
        row = cursor.fetchone()
        if row and row[0]:
            statements.append(clean_metadata_ddl(row[0]))
    return statements


def existing_constraint_statements(cursor: oracledb.Cursor, target_owner: str, target_table: str) -> list[str]:
    cursor.execute(
        """
        SELECT constraint_name
          FROM user_constraints
         WHERE table_name = :table_name
           AND constraint_type IN ('P', 'U')
         ORDER BY constraint_name
        """,
        {"table_name": target_table},
    )
    statements: list[str] = []
    for (constraint_name,) in cursor.fetchall():
        cursor.execute(
            "SELECT DBMS_METADATA.GET_DDL('CONSTRAINT', :constraint_name, :owner) FROM DUAL",
            {"constraint_name": constraint_name, "owner": target_owner},
        )
        row = cursor.fetchone()
        if row and row[0]:
            statements.append(clean_metadata_ddl(row[0]))
    return statements


def build_table_ddl(
    target_owner: str,
    target_table: str,
    owner: str,
    source_table: str,
    entity: str,
    layout: pd.DataFrame,
    table_tablespace: str,
    index_tablespace: str,
    character_multiplier: int,
    initial_size_gb: float | None,
) -> tuple[str, list[str]]:
    used_columns: set[str] = set()
    column_lines: list[str] = []
    column_comments: list[str] = []
    for _, row in layout.sort_values("COLNO", key=lambda item: item.map(colno_number)).iterrows():
        source_name = clean_text(row["COL"])
        source_attr = clean_text(row["ATTR"])
        oracle_name = source_column_name(source_name, row["COLNO"], used_columns)
        oracle_identifier = oracle_column_identifier(oracle_name)
        oracle_type = mssql_to_oracle(row["DATATYPE"], row["LEN"], character_multiplier)
        column_lines.append(f"    {oracle_identifier} {oracle_type}")
        if source_attr:
            column_comments.append(f"COMMENT ON COLUMN {target_owner}.{target_table}.{oracle_identifier} IS {q_literal(source_attr)}")
    ddl = f"CREATE TABLE {target_owner}.{target_table} (\n"
    ddl += ",\n".join(column_lines)
    ddl += f"\n) SEGMENT CREATION IMMEDIATE TABLESPACE {table_tablespace}{storage_clause(initial_size_gb)}"
    table_comment = clean_text(entity)
    comments = ([] if not table_comment else [f"COMMENT ON TABLE {target_owner}.{target_table} IS {q_literal(table_comment)}"]) + column_comments
    return ddl, comments


def build_ddl_artifact(
    table_changes: pd.DataFrame,
    before_layout: pd.DataFrame,
    after_layout: pd.DataFrame,
    target_owner: str,
    table_tablespace: str,
    index_tablespace: str,
    character_multiplier: int,
) -> GeneratedDdl:
    before_groups = table_groups(before_layout)
    after_groups = table_groups(after_layout)
    statements: list[DdlStatement] = []
    existing_tables = [
        target_table_name(normalized(change["DB"]), normalized(change["테이블"]))
        for _, change in table_changes.iterrows()
        if clean_text(change["구분"]) == "변경"
    ]
    current_sizes = fetch_current_table_sizes(target_owner, sorted(set(existing_tables)))
    with get_pool().acquire() as conn:
        with conn.cursor() as cursor:
            for _, change in table_changes.iterrows():
                source_owner = normalized(change["DB"])
                source_table = normalized(change["테이블"])
                status = clean_text(change["구분"])
                source_layout = after_groups.get((source_owner, source_table))
                target_table = target_table_name(source_owner, source_table)
                if status in {"변경", "삭제"}:
                    statements.append(
                        DdlStatement(
                            "DROP",
                            source_owner,
                            source_table,
                            target_table,
                            f"DROP TABLE {target_owner}.{target_table} CASCADE CONSTRAINTS PURGE",
                        )
                    )
                if status == "삭제" or source_layout is None or source_layout.empty:
                    continue
                entity = first_entity(source_layout)
                create_sql, comment_sqls = build_table_ddl(
                    target_owner,
                    target_table,
                    source_owner,
                    source_table,
                    entity,
                    source_layout,
                    table_tablespace,
                    index_tablespace,
                    character_multiplier,
                    current_sizes.get(target_table) if status == "변경" else None,
                )
                statements.append(DdlStatement("CREATE TABLE", source_owner, source_table, target_table, create_sql))
                for comment_sql in comment_sqls:
                    statements.append(DdlStatement("COMMENT", source_owner, source_table, target_table, comment_sql))
                if status == "변경":
                    try:
                        constraint_statements = existing_constraint_statements(cursor, target_owner, target_table)
                    except Exception as exc:
                        raise RuntimeError(
                            f"기존 PK·UNIQUE 제약 추출 실패: {target_owner}.{target_table}. USER_CONSTRAINTS 또는 DBMS_METADATA 조회 오류입니다. 원본 오류: {exc}"
                        ) from exc
                    try:
                        index_statements = existing_index_statements(cursor, target_owner, target_table)
                    except Exception as exc:
                        raise RuntimeError(
                            f"기존 인덱스 추출 실패: {target_owner}.{target_table}. USER_INDEXES 또는 DBMS_METADATA 조회 오류입니다. 원본 오류: {exc}"
                        ) from exc
                    for constraint_sql in constraint_statements:
                        statements.append(DdlStatement("CREATE CONSTRAINT", source_owner, source_table, target_table, constraint_sql))
                    for index_sql in index_statements:
                        statements.append(DdlStatement("CREATE INDEX", source_owner, source_table, target_table, index_sql))
    def statement_text(items: list[DdlStatement]) -> str:
        blocks: list[str] = []
        current_key: tuple[str, str] | None = None
        current_lines: list[str] = []
        for statement in items:
            statement_key = (statement.source_db, statement.source_table)
            if current_key is not None and statement_key != current_key:
                blocks.append("\n".join(current_lines))
                current_lines = []
            current_key = statement_key
            current_lines.append(f"{statement.sql};")
        if current_lines:
            blocks.append("\n".join(current_lines))
        return "\n\n".join(blocks) + ("\n" if blocks else "")

    text = statement_text(statements)
    structure_text = statement_text([statement for statement in statements if statement.action != "COMMENT"])
    comment_statements = [statement for statement in statements if statement.action == "COMMENT"]
    comment_text = statement_text(comment_statements)
    source_keys = tuple(sorted({(statement.source_db, statement.source_table) for statement in statements}))
    return GeneratedDdl(
        tuple(statements),
        text,
        structure_text,
        comment_text,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        len(source_keys),
        len(comment_statements),
        source_keys,
    )


def generated_table_grid(artifact: GeneratedDdl) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for source_db, source_table in artifact.source_keys:
        statements = [item for item in artifact.statements if (item.source_db, item.source_table) == (source_db, source_table)]
        target_table = statements[0].target_table if statements else ""
        rows.append({"DB": source_db, "테이블": source_table, "대상 Oracle 테이블": target_table, "DDL 구문 수": len(statements)})
    return pd.DataFrame(rows)


def render_ddl_preview(ddl_text: str) -> None:
    lines = ddl_text.splitlines()
    preview = "\n".join(lines[:DDL_PREVIEW_MAX_LINES])
    if len(lines) > DDL_PREVIEW_MAX_LINES:
        st.warning(
            f"미리보기는 처음 {DDL_PREVIEW_MAX_LINES:,}행만 표시합니다. "
            f"전체 {len(lines):,}행은 다운로드 파일에서 확인해 주세요."
        )
    st.code(preview, language="sql")


def execute_ddl(artifact: GeneratedDdl, selected_keys: set[tuple[str, str]]) -> pd.DataFrame:
    selected_statements = [item for item in artifact.statements if (item.source_db, item.source_table) in selected_keys]
    status_by_table: dict[tuple[str, str], dict[str, str | int]] = {}
    for statement in selected_statements:
        key = (statement.source_db, statement.source_table)
        status_by_table.setdefault(
            key,
            {"DB": statement.source_db, "테이블": statement.source_table, "대상 Oracle 테이블": statement.target_table, "실행 구문 수": 0, "결과": "성공", "메시지": ""},
        )
    failed_tables: set[tuple[str, str]] = set()
    progress = st.progress(0.0)
    with get_pool().acquire() as conn:
        with conn.cursor() as cursor:
            for number, statement in enumerate(selected_statements, start=1):
                row = status_by_table[(statement.source_db, statement.source_table)]
                table_key = (statement.source_db, statement.source_table)
                if table_key in failed_tables:
                    progress.progress(number / max(1, len(selected_statements)))
                    continue
                try:
                    cursor.execute(statement.sql)
                except Exception as exc:
                    row["결과"] = "실패"
                    row["메시지"] = str(exc)
                    failed_tables.add(table_key)
                row["실행 구문 수"] = int(row["실행 구문 수"]) + 1
                progress.progress(number / max(1, len(selected_statements)))
        conn.commit()
    progress.empty()
    return pd.DataFrame(status_by_table.values())


def render_header() -> None:
    st.markdown(
        """
        <style>
        .block-container { max-width: 1640px; padding-top: 1.4rem; padding-bottom: 2.5rem; }
        .tlp-hero { background: linear-gradient(115deg, #102A43 0%, #005EA8 48%, #00A6A6 100%); border-radius: 16px; color: white; padding: 28px 32px; text-align: center; box-shadow: 0 12px 28px rgba(0, 94, 168, 0.20); margin-bottom: 1.4rem; }
        .tlp-hero-title { font-size: 30px; font-weight: 750; letter-spacing: -0.6px; }
        .tlp-hero-subtitle { margin-top: 8px; font-size: 14px; color: #E6F6FF; }
        .tlp-hero-credit { margin-top: 12px; font-size: 12px; color: #C6F6F6; }
        </style>
        <div class="tlp-hero">
            <div class="tlp-hero-title">TLP DB 변경 내역</div>
            <div class="tlp-hero-subtitle">MSSQL 레이아웃 비교 · Oracle 스테이징 DDL 생성</div>
            <div class="tlp-hero-credit">⚙️ Created by ♡홍율파파♡</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_comparison() -> tuple[list[str], pd.DataFrame]:
    comparison = st.session_state.comparison
    if comparison is None:
        st.info("비교 기준일과 대상 기준일을 선택한 뒤 변경 내역 조회를 실행해 주십시오.", icon=":material/info:")
        return [], pd.DataFrame()
    table_changes: pd.DataFrame = comparison["table_changes"]
    column_changes: pd.DataFrame = comparison["column_changes"]
    timing = comparison.get("timing")
    if timing:
        st.caption(
            f"레이아웃 조회 {timing['load_seconds']:.1f}초 · 변경 분석 {timing['compare_seconds']:.1f}초 · 대상 행 {timing['row_count']:,}건"
        )
    if table_changes.empty:
        st.success("두 기준일 사이에 테이블 및 컬럼 변경이 없습니다.", icon=":material/check_circle:")
        return [], pd.DataFrame()
    summary = (
        table_changes.pivot_table(index="DB", columns="구분", values="테이블", aggfunc="count", fill_value=0)
        .reindex(columns=["신규", "삭제", "변경"], fill_value=0)
        .reset_index()
    )
    summary["합계"] = summary[["신규", "삭제", "변경"]].sum(axis=1)
    summary = pd.concat(
        [summary, pd.DataFrame([{"DB": "합계", "신규": summary["신규"].sum(), "삭제": summary["삭제"].sum(), "변경": summary["변경"].sum(), "합계": summary["합계"].sum()}])]
    )
    owners = sorted(table_changes["DB"].unique().tolist())
    summary_col, table_col = st.columns((0.85, 1.55))
    with summary_col:
        with st.container(border=True):
            st.markdown("#### :material/analytics: DB별 요약")
            st.caption("복수 선택 가능 · 미선택 또는 합계 선택 시 전체 DB")
            selection = st.dataframe(
                summary,
                hide_index=True,
                on_select="rerun",
                selection_mode="multi-row",
                key="db_summary",
                column_config={"합계": st.column_config.NumberColumn(format="%d")},
                height=430,
            )
    selected_rows = selection.selection.rows
    selected_owners = [clean_text(summary.iloc[row]["DB"]) for row in selected_rows]
    if not selected_owners or "합계" in selected_owners:
        selected_owners = owners
    selected_tables = table_changes[table_changes["DB"].isin(selected_owners)].copy()
    selected_columns = column_changes[column_changes["DB"].isin(selected_owners)].copy()
    with table_col:
        with st.container(border=True):
            st.markdown("#### :material/table_chart: 테이블 변경 내역")
            st.caption("DDL을 만들 테이블 행을 하나 이상 선택해 주십시오.")
            table_selection = st.dataframe(
                selected_tables,
                hide_index=True,
                height=430,
                on_select="rerun",
                selection_mode="multi-row",
                key="table_selection",
            )
    selected_rows = table_selection.selection.rows
    ddl_tables = selected_tables.iloc[selected_rows].copy() if selected_rows else pd.DataFrame(columns=selected_tables.columns)
    selected_table_keys = {(normalized(row["DB"]), normalized(row["테이블"])) for _, row in ddl_tables.iterrows()}
    column_display = selected_columns[
        selected_columns.apply(lambda row: (normalized(row["DB"]), normalized(row["테이블"])) in selected_table_keys, axis=1)
    ].copy() if selected_table_keys else selected_columns
    with st.container(border=True):
        st.markdown("#### :material/view_column: 컬럼 변경 내역")
        st.caption("테이블을 선택하면 선택 테이블의 컬럼 변경 내역만 표시합니다.")
        st.dataframe(column_display, hide_index=True, height=430)
    return selected_owners, ddl_tables


def render_ddl_controls(selected_owners: list[str], selected_tables: pd.DataFrame, character_multiplier: int) -> None:
    if not selected_owners:
        return
    comparison = st.session_state.comparison
    if comparison is None:
        return
    selected_keys = tuple(sorted((normalized(row["DB"]), normalized(row["테이블"])) for _, row in selected_tables.iterrows()))
    with st.container(border=True):
        st.subheader(":material/code: Oracle DDL 생성")
        st.caption(f"선택 테이블: {len(selected_keys)}개")
        multiplier = st.number_input("문자형 길이 배수", min_value=1.0, max_value=4.0, value=float(character_multiplier), step=0.5, key="character_multiplier")
        if st.button("선택 테이블 DDL 생성", type="primary", icon=":material/code:", disabled=not selected_keys):
            try:
                st.session_state.ddl_artifact = build_ddl_artifact(
                    selected_tables,
                    comparison["before_layout"],
                    comparison["after_layout"],
                    DEFAULT_TARGET_OWNER,
                    DEFAULT_TABLESPACE,
                    DEFAULT_INDEX_TABLESPACE,
                    int(multiplier * 10) / 10,
                )
                st.session_state.ddl_logs = None
            except Exception as exc:
                st.error(str(exc))
    artifact: GeneratedDdl | None = st.session_state.ddl_artifact
    if artifact is None or artifact.source_keys != selected_keys:
        return
    st.success(f"DDL 생성 테이블: {artifact.table_count}개 · 생성 시각: {artifact.generated_at}", icon=":material/check_circle:")
    structure_tab, comment_tab = st.tabs([":material/account_tree: 구조 DDL", ":material/comment: 코멘트 DDL"])
    with structure_tab:
        st.caption("구조 DDL에는 테이블·컬럼 코멘트가 함께 포함됩니다.")
        st.download_button(
            "구조 DDL 다운로드",
            data=artifact.text,
            file_name=f"MssqlOracleLayout_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/sql",
            icon=":material/download:",
        )
        if st.toggle("구조 DDL 미리보기", value=False, key="ddl_structure_preview"):
            render_ddl_preview(artifact.text)
    with comment_tab:
        st.caption(f"코멘트만 별도로 적용할 때 사용하는 DDL입니다. 테이블·컬럼 코멘트 {artifact.comment_count}건 · 컬럼 코멘트는 레이아웃의 ATTR 값을 사용합니다.")
        if artifact.comment_text:
            st.download_button(
                "코멘트 DDL 다운로드",
                data=artifact.comment_text,
                file_name=f"MssqlOracleLayout_Comments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
                mime="text/sql",
                icon=":material/download:",
            )
            if st.toggle("코멘트 DDL 미리보기", value=False, key="ddl_comment_preview"):
                render_ddl_preview(artifact.comment_text)
        else:
            st.info("생성할 코멘트가 없습니다. ENTITY 또는 ATTR 값이 있는지 확인해 주세요.")
    with st.expander(":material/play_arrow: Oracle DDL 실행", expanded=False):
        st.warning("변경·삭제 대상에는 DROP TABLE이 실행됩니다. 실행 실패 SQL은 계속 진행한 뒤 결과를 남깁니다.")
        execution_targets = generated_table_grid(artifact)
        st.caption("실행할 테이블 행을 선택해 주십시오.")
        execution_selection = st.dataframe(
            execution_targets,
            hide_index=True,
            on_select="rerun",
            selection_mode="multi-row",
            key="execution_table_selection",
            height=260,
        )
        execution_rows = execution_selection.selection.rows
        execution_keys = {
            (normalized(execution_targets.iloc[row]["DB"]), normalized(execution_targets.iloc[row]["테이블"]))
            for row in execution_rows
        }
        confirmed = st.checkbox("생성된 DDL의 DROP 및 CREATE 실행을 확인했습니다.", key="execute_confirm")
        if st.button("선택 테이블 Oracle DDL 실행", type="primary", icon=":material/play_arrow:", disabled=not confirmed or not execution_keys):
            st.session_state.ddl_logs = execute_ddl(artifact, execution_keys)
    if st.session_state.ddl_logs is not None:
        st.subheader(":material/fact_check: DDL 실행 결과")
        st.dataframe(st.session_state.ddl_logs, hide_index=True, height=320)


def main() -> None:
    st.set_page_config(page_title="TLP DB 변경 내역", page_icon=":material/difference:", layout="wide")
    init_state()
    render_header()
    layout_owner = sql_name(str(st.secrets["oracle"]["user"]))
    layout_table = DEFAULT_LAYOUT_TABLE
    try:
        snapshot_dates = fetch_snapshot_dates(layout_owner, layout_table)
    except Exception as exc:
        st.error(f"레이아웃 기준일을 조회할 수 없습니다: {exc}")
        st.stop()
    if len(snapshot_dates) < 2:
        st.warning("비교할 기준일이 두 개 이상 필요합니다.")
        st.stop()
    labels = [snapshot_label(value) for value in snapshot_dates]
    default_before_index = labels.index("2026-07-03") if "2026-07-03" in labels else max(0, len(labels) - 2)
    st.subheader(":material/date_range: 변경 내역 조회")
    with st.form("comparison_form", border=True):
        before_col, after_col, action_col = st.columns((1, 1, 0.7), vertical_alignment="bottom")
        with before_col:
            before_label = st.selectbox("비교 기준일", labels, index=default_before_index)
        with after_col:
            after_label = st.selectbox("대상 기준일", labels, index=len(labels) - 1)
        with action_col:
            submitted = st.form_submit_button("변경 내역 조회", type="primary", icon=":material/search:")
    if submitted:
        if before_label == after_label:
            st.error("서로 다른 두 기준일을 선택해 주십시오.")
        else:
            try:
                before_token = snapshot_dates[labels.index(before_label)]
                after_token = snapshot_dates[labels.index(after_label)]
                with st.status("변경 내역 조회를 시작합니다.", expanded=True) as status:
                    status.write("두 기준일의 레이아웃 데이터를 읽고 있습니다.")
                    load_started = perf_counter()
                    before_layout, after_layout = fetch_layout_pair(layout_owner, layout_table, before_token, after_token)
                    load_seconds = perf_counter() - load_started
                    status.write(f"레이아웃 조회 완료: {len(before_layout) + len(after_layout):,}건. 테이블·컬럼 변경을 분석하고 있습니다.")
                    compare_started = perf_counter()
                    table_changes, column_changes = compare_layouts(before_layout, after_layout)
                    compare_seconds = perf_counter() - compare_started
                    status.update(
                        label=f"변경 내역 분석 완료: 테이블 {len(table_changes):,}건 · 컬럼 {len(column_changes):,}건",
                        state="complete",
                        expanded=False,
                    )
                st.session_state.comparison = {
                    "before_layout": before_layout,
                    "after_layout": after_layout,
                    "table_changes": table_changes,
                    "column_changes": column_changes,
                    "timing": {
                        "load_seconds": load_seconds,
                        "compare_seconds": compare_seconds,
                        "row_count": len(before_layout) + len(after_layout),
                    },
                }
                st.session_state.ddl_artifact = None
                st.session_state.ddl_logs = None
                st.session_state.selected_owner = None
            except Exception as exc:
                st.error(f"변경 내역 조회에 실패했습니다: {exc}")
    selected_owner, selected_tables = render_comparison()
    render_ddl_controls(selected_owner, selected_tables, 3)


if __name__ == "__main__":
    main()

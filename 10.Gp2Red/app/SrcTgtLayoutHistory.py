from __future__ import annotations

import re
import sys
from datetime import date
from time import perf_counter
from typing import Any

import pandas as pd
import streamlit as st

from SrcTgtSecurity import allowed, require_access

try:
    import psycopg
except ImportError:
    psycopg = None


LAYOUT_COLUMNS = ["STD_DT", "OWNER", "TBL", "ENTITY", "COLNO", "COL", "ATTR", "DATATYPE", "LEN", "ISPK", "NULLABLE"]


def text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def normalized(value: object) -> str:
    return re.sub(r"\s+", " ", text(value)).upper()


def normalized_series(values: pd.Series) -> pd.Series:
    return values.astype("string").fillna("").str.strip().str.replace(r"\s+", " ", regex=True).str.upper()


def identifier(value: str) -> str:
    name = text(value)
    if not name or "\x00" in name or len(name.encode("utf-8")) > 127:
        raise ValueError(f"식별자 형식이 올바르지 않습니다: {value}")
    return '"' + name.replace('"', '""') + '"'


def qualified(schema_name: str, table_name: str) -> str:
    return f"{identifier(schema_name)}.{identifier(table_name)}"


def config(section: str) -> dict[str, Any]:
    required = ("host", "port", "database", "user", "password")
    if section not in st.secrets:
        raise ValueError(f".streamlit/secrets.toml에 [{section}] 설정이 없습니다.")
    values = dict(st.secrets[section])
    missing = [key for key in required if not text(values.get(key))]
    if missing:
        raise ValueError(f"[{section}] 필수 항목이 없습니다: {', '.join(missing)}")
    return values


def layout_settings(target: dict[str, Any]) -> tuple[str, str]:
    values = dict(st.secrets.get("layout_history", {}))
    return text(values.get("schema")) or text(target.get("default_schema")) or "public", text(values.get("table")) or "TB_TABLE_LAYOUT_GP"


def connect(values: dict[str, Any]) -> Any:
    if psycopg is None:
        raise RuntimeError(f"psycopg가 현재 실행 Python에 설치되지 않았습니다: {sys.executable}")
    arguments: dict[str, Any] = {"host": text(values["host"]), "port": int(values["port"]), "dbname": text(values["database"]), "user": text(values["user"]), "password": text(values["password"]), "connect_timeout": int(values.get("connect_timeout", 15))}
    if text(values.get("sslmode")):
        arguments["sslmode"] = text(values["sslmode"])
    return psycopg.connect(**arguments)


def init_state() -> None:
    for key, value in {"gp_layout_comparison": None, "gp_layout_capture": None}.items():
        st.session_state.setdefault(key, value)


def apply_green_style() -> None:
    st.markdown("""<style>
    [data-testid="stAppViewContainer"]{background:radial-gradient(circle at 18% -10%,#1d6648 0,#10271d 35%,#07140f 100%)}
    [data-testid="stHeader"]{background:transparent}.block-container{max-width:1520px;padding-top:2.1rem}
    .hero{padding:.8rem 1.05rem;margin-bottom:.8rem;background:linear-gradient(105deg,#195b40,#102b20);border:1px solid #34815d;border-left:4px solid #7de8ac;border-radius:10px;color:#f7fffa}.hero h1{margin:0;font-size:1.28rem;line-height:1.2;letter-spacing:-.02em}.hero p{margin:.3rem 0 0;color:#b9dcc8;font-size:.72rem}
    [data-testid="stDataFrame"]{border:1px solid #3e7555;border-radius:9px;overflow:hidden}[data-testid="stMetric"]{background:#173224;border:1px solid #3e7555;border-radius:9px}[data-testid="stButton"]>button{border-color:#4a9a6d;background:#1f6848;color:#fff}[data-testid="stButton"]>button[kind="primary"]{background:linear-gradient(100deg,#267c55,#4fba7a)}[data-testid="stProgressBar"]>div>div{background:linear-gradient(90deg,#2f8d61,#83dc9f)}</style>""", unsafe_allow_html=True)


def snapshot_label(value: object) -> str:
    raw = text(value)
    digits = re.sub(r"\D", "", raw)
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:]}" if len(digits) == 8 else raw


@st.cache_data(ttl=120, show_spinner=False)
def fetch_snapshot_dates(target_key: tuple[str, str, str, int], schema_name: str, table_name: str) -> list[str]:
    target = {"host": target_key[0], "database": target_key[1], "user": target_key[2], "port": target_key[3], "password": st.secrets["redshift_sql"]["password"], "sslmode": st.secrets["redshift_sql"].get("sslmode", ""), "connect_timeout": st.secrets["redshift_sql"].get("connect_timeout", 15)}
    with connect(target) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT DISTINCT STD_DT FROM {qualified(schema_name, table_name)} ORDER BY STD_DT")
            return [text(row[0]) for row in cursor.fetchall() if text(row[0])]


def dataframe_from_rows(rows: list[tuple[Any, ...]], columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=columns)


def fetch_layout_pair(target: dict[str, Any], schema_name: str, table_name: str, before_date: str, after_date: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    query = f"SELECT {', '.join(LAYOUT_COLUMNS)} FROM {qualified(schema_name, table_name)} WHERE STD_DT IN (%s, %s) ORDER BY OWNER, TBL, COLNO"
    with connect(target) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (before_date, after_date))
            frame = dataframe_from_rows(cursor.fetchall(), LAYOUT_COLUMNS)
    return frame[frame.STD_DT.map(text) == before_date].copy(), frame[frame.STD_DT.map(text) == after_date].copy()


def colno_order(value: object) -> int:
    try:
        return int(float(text(value)))
    except ValueError:
        return 999999999


def table_groups(frame: pd.DataFrame) -> dict[tuple[str, str], pd.DataFrame]:
    prepared = frame.copy()
    prepared["_OWNER"] = prepared.OWNER.map(normalized)
    prepared["_TABLE"] = prepared.TBL.map(normalized)
    prepared["_ORDER"] = prepared.COLNO.map(colno_order)
    for column in ("COLNO", "COL", "ATTR", "DATATYPE", "LEN", "ISPK", "NULLABLE"):
        prepared[f"_SIG_{column}"] = normalized_series(prepared[column])
    return {key: group.copy() for key, group in prepared.groupby(["_OWNER", "_TABLE"], dropna=False, sort=False)}


def entity(frame: pd.DataFrame) -> str:
    values = [text(value) for value in frame.ENTITY.tolist() if text(value)] if not frame.empty else []
    return values[0] if values else ""


def table_signature(frame: pd.DataFrame) -> tuple[tuple[str, ...], ...]:
    columns = ["_SIG_COLNO", "_SIG_COL", "_SIG_ATTR", "_SIG_DATATYPE", "_SIG_LEN", "_SIG_ISPK", "_SIG_NULLABLE"]
    return tuple(frame.sort_values("_ORDER")[columns].itertuples(index=False, name=None))


def compare_layouts(before: pd.DataFrame, after: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    before_groups, after_groups = table_groups(before), table_groups(after)
    tables: list[dict[str, str]] = []
    columns: list[dict[str, str]] = []
    for owner_name, table_name in sorted(set(before_groups) | set(after_groups)):
        previous = before_groups.get((owner_name, table_name), pd.DataFrame(columns=before.columns))
        current = after_groups.get((owner_name, table_name), pd.DataFrame(columns=after.columns))
        status = "신규" if previous.empty else "삭제" if current.empty else "변경" if table_signature(previous) != table_signature(current) or entity(previous) != entity(current) else ""
        if not status:
            continue
        tables.append({"스키마": owner_name, "테이블": table_name, "엔티티(전)": entity(previous), "엔티티(후)": entity(current), "구분": status})
        previous_by_order = {normalized(row.COLNO): row for row in previous.itertuples(index=False)}
        current_by_order = {normalized(row.COLNO): row for row in current.itertuples(index=False)}
        for order in sorted(set(previous_by_order) | set(current_by_order), key=colno_order):
            old, new = previous_by_order.get(order), current_by_order.get(order)
            names = ("COL", "ATTR", "DATATYPE", "LEN", "ISPK", "NULLABLE")
            if old is not None and new is not None and all(normalized(getattr(old, name)) == normalized(getattr(new, name)) for name in names):
                continue
            columns.append({"스키마": owner_name, "테이블": table_name, "엔티티": entity(current) or entity(previous), "구분": "신규" if old is None else "삭제" if new is None else "변경", "컬럼순서": order, "컬럼명(전)": text(getattr(old, "COL", "")), "컬럼명(후)": text(getattr(new, "COL", "")), "속성(전)": text(getattr(old, "ATTR", "")), "속성(후)": text(getattr(new, "ATTR", "")), "타입(전)": text(getattr(old, "DATATYPE", "")), "타입(후)": text(getattr(new, "DATATYPE", "")), "길이(전)": text(getattr(old, "LEN", "")), "길이(후)": text(getattr(new, "LEN", "")), "PK(전)": text(getattr(old, "ISPK", "")), "PK(후)": text(getattr(new, "ISPK", "")), "NOT NULL(전)": text(getattr(old, "NULLABLE", "")), "NOT NULL(후)": text(getattr(new, "NULLABLE", ""))})
    return pd.DataFrame(tables), pd.DataFrame(columns)


def list_source_schemas(source: dict[str, Any]) -> list[str]:
    query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema','pg_catalog') AND schema_name NOT LIKE 'pg_%' ORDER BY schema_name"
    with connect(source) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return [text(row[0]) for row in cursor.fetchall()]


def fetch_source_layout(source: dict[str, Any], schemas: list[str], standard_date: str) -> pd.DataFrame:
    if not schemas:
        raise ValueError("원천 스키마를 한 개 이상 선택하십시오.")
    query = """
        SELECT %s, c.table_schema, c.table_name, COALESCE(obj_description(pc.oid, 'pg_class'), ''),
               c.ordinal_position, c.column_name, COALESCE(col_description(pc.oid, c.ordinal_position), ''),
               c.data_type, COALESCE(c.character_maximum_length::text, c.numeric_precision::text, ''),
               CASE WHEN pk.column_name IS NULL THEN '' ELSE 'Y' END, c.is_nullable
          FROM information_schema.columns c
          JOIN pg_namespace pn ON pn.nspname = c.table_schema
          JOIN pg_class pc ON pc.relnamespace = pn.oid AND pc.relname = c.table_name
          LEFT JOIN (
              SELECT kcu.table_schema, kcu.table_name, kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON kcu.constraint_name = tc.constraint_name
                 AND kcu.table_schema = tc.table_schema
               WHERE tc.constraint_type = 'PRIMARY KEY'
          ) pk ON pk.table_schema = c.table_schema AND pk.table_name = c.table_name AND pk.column_name = c.column_name
         WHERE c.table_schema = ANY(%s)
         ORDER BY c.table_schema, c.table_name, c.ordinal_position
    """
    with connect(source) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (standard_date, schemas))
            return dataframe_from_rows(cursor.fetchall(), LAYOUT_COLUMNS)


def save_layout(target: dict[str, Any], schema_name: str, table_name: str, standard_date: str, selected_schemas: list[str], layout: pd.DataFrame) -> int:
    if layout.empty:
        raise ValueError("적재할 원천 레이아웃이 없습니다.")
    owners = [text(value) for value in selected_schemas if text(value)]
    if not owners:
        raise ValueError("삭제할 원천 스키마를 선택하십시오.")
    insert = f"INSERT INTO {qualified(schema_name, table_name)} ({', '.join(LAYOUT_COLUMNS)}) VALUES ({', '.join('%s' for _ in LAYOUT_COLUMNS)})"
    owner_placeholders = ", ".join("%s" for _ in owners)
    with connect(target) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM {qualified(schema_name, table_name)} WHERE STD_DT=%s AND OWNER IN ({owner_placeholders})", (standard_date, *owners))
            cursor.executemany(insert, [tuple(row) for row in layout[LAYOUT_COLUMNS].itertuples(index=False, name=None)])
        connection.commit()
    return len(layout)


def number_columns(frame: pd.DataFrame) -> dict[str, Any]:
    return {column: st.column_config.NumberColumn(format="localized") for column in frame.columns if pd.api.types.is_numeric_dtype(frame[column])}


def ddl_identifier(value: object) -> str:
    name = text(value)
    if not name:
        raise ValueError("DDL 식별값이 없습니다.")
    return '"' + name.replace('"', '""') + '"'


def ddl_data_type(row: pd.Series) -> str:
    data_type, length = text(row.DATATYPE).upper(), text(row.LEN)
    if not data_type:
        raise ValueError("원천 데이터 타입이 없습니다.")
    if length and "(" not in data_type and data_type.lower() in {"character varying", "varchar", "character", "char", "numeric", "decimal"}:
        return f"{data_type}({length})"
    return data_type


def reference_ddl(owner: str, table: str, before: pd.DataFrame, after: pd.DataFrame) -> str:
    qualified_name = f"{ddl_identifier(owner)}.{ddl_identifier(table)}"
    drop = f"DROP TABLE IF EXISTS {qualified_name};"
    if after.empty:
        return drop
    definitions = []
    for row in after.sort_values("COLNO", key=lambda values: values.map(colno_order)).itertuples(index=False):
        null_clause = "" if normalized(row.NULLABLE) in {"YES", "Y", "TRUE"} else " NOT NULL"
        definitions.append(f"    {ddl_identifier(row.COL)} {ddl_data_type(pd.Series(row._asdict()))}{null_clause}")
    primary_key = [ddl_identifier(row.COL) for row in after.sort_values("COLNO", key=lambda values: values.map(colno_order)).itertuples(index=False) if normalized(row.ISPK) in {"Y", "YES", "TRUE"}]
    if primary_key:
        definitions.append(f"    PRIMARY KEY ({', '.join(primary_key)})")
    body = ",\n".join(definitions)
    return f"{drop}\n\nCREATE TABLE {qualified_name} (\n{body}\n);"


def render_comparison() -> None:
    comparison = st.session_state.gp_layout_comparison
    if comparison is None:
        return
    table_changes, column_changes = comparison["tables"], comparison["columns"]
    if table_changes.empty:
        st.success("변경 내역이 없습니다.", icon=":material/check_circle:")
        return
    summary = table_changes.pivot_table(index="스키마", columns="구분", values="테이블", aggfunc="count", fill_value=0).reindex(columns=["신규", "삭제", "변경"], fill_value=0).reset_index()
    summary["합계"] = summary[["신규", "삭제", "변경"]].sum(axis=1)
    summary = pd.concat([summary, pd.DataFrame([{"스키마":"합계", "신규":summary["신규"].sum(), "삭제":summary["삭제"].sum(), "변경":summary["변경"].sum(), "합계":summary["합계"].sum()}])], ignore_index=True)
    left, right = st.columns((0.8, 1.6))
    with left:
        with st.container(border=True):
            st.markdown("#### :material/analytics: 요약")
            event = st.dataframe(summary, hide_index=True, on_select="rerun", selection_mode="multi-row", key="gp_layout_schema", height=380, column_config=number_columns(summary))
    selected = [text(summary.iloc[index]["스키마"]) for index in event.selection.rows]
    schemas = sorted(table_changes["스키마"].unique()) if not selected or "합계" in selected else selected
    selected_tables = table_changes[table_changes["스키마"].isin(schemas)].copy()
    with right:
        with st.container(border=True):
            st.markdown("#### :material/table_chart: 테이블")
            table_event = st.dataframe(selected_tables, hide_index=True, on_select="rerun", selection_mode="multi-row", key="gp_layout_table", height=380)
    selected_keys = {(normalized(selected_tables.iloc[index]["스키마"]), normalized(selected_tables.iloc[index]["테이블"])) for index in table_event.selection.rows}
    shown_columns = column_changes[column_changes.apply(lambda row: (normalized(row["스키마"]), normalized(row["테이블"])) in selected_keys, axis=1)] if selected_keys else column_changes
    with st.container(border=True):
        st.markdown("#### :material/view_column: 컬럼")
        st.dataframe(shown_columns, hide_index=True, height=380, column_config=number_columns(shown_columns))
    candidate_keys = selected_keys or {(normalized(row["스키마"]), normalized(row["테이블"])) for _, row in selected_tables.iterrows()}
    if candidate_keys:
        options = sorted(candidate_keys)
        selected_ddl = st.selectbox("DDL 참조 테이블", options, format_func=lambda value: f"{value[0]}.{value[1]}", key="gp_layout_ddl_table")
        before = comparison["before"]
        after = comparison["after"]
        owner, table = selected_ddl
        ddl = reference_ddl(owner, table, before[(before.OWNER.map(normalized) == owner) & (before.TBL.map(normalized) == table)], after[(after.OWNER.map(normalized) == owner) & (after.TBL.map(normalized) == table)])
        with st.container(border=True):
            st.markdown("#### :material/code: DBA 참조 DDL")
            st.code(ddl, language="sql")
            st.download_button("DDL 다운로드", ddl, file_name=f"{owner}_{table}_reference.sql", mime="text/sql", icon=":material/download:")


def main() -> None:
    access = require_access()
    if not allowed(access.authorizations, "EDIT"):
        st.error("원천 레이아웃 수집 권한이 없습니다.", icon=":material/lock:")
        st.stop()
    init_state(); apply_green_style()
    try:
        source, target = config("greenplum"), config("redshift_sql")
    except Exception as error:
        st.error(str(error), icon=":material/error:")
        st.stop()
    schema_name, table_name = layout_settings(target)
    st.markdown('<div class="hero"><h1>✦ 원천 레이아웃 이력</h1><p>⚙️ Created by ♡홍율파파♡</p></div>', unsafe_allow_html=True)
    try:
        target_key = (text(target["host"]), text(target["database"]), text(target["user"]), int(target["port"]))
        dates = fetch_snapshot_dates(target_key, schema_name, table_name)
    except Exception as error:
        st.error(f"레이아웃 기준일 조회 실패: {error}", icon=":material/error:")
        st.stop()
    labels = [snapshot_label(value) for value in dates]
    with st.sidebar:
        st.header(":material/tune: 조회 조건")
        if len(dates) >= 2:
            before_label = st.selectbox("비교 기준일", labels, index=max(0, len(labels)-2))
            after_label = st.selectbox("대상 기준일", labels, index=len(labels)-1)
            compared = st.button("변경 내역 조회", type="primary", icon=":material/search:", width="stretch")
        else:
            before_label = after_label = ""
            compared = False
            st.info("기준일 두 건 이상 필요", icon=":material/info:")
        with st.expander(":material/database_upload: 당일 레이아웃 적재", expanded=False):
            try:
                schemas = list_source_schemas(source)
                selected_schemas = st.multiselect("원천 스키마", schemas, default=[text(source.get("default_schema"))] if text(source.get("default_schema")) in schemas else [])
                standard_day = st.date_input("기준일", value=date.today(), format="YYYY-MM-DD")
                captured = st.button("레이아웃 적재", type="primary", icon=":material/upload:", width="stretch")
            except Exception as error:
                st.error(str(error), icon=":material/error:")
                captured = False
                selected_schemas = []
                standard_day = date.today()
    if compared:
        if before_label == after_label:
            st.error("서로 다른 기준일을 선택하십시오.")
        else:
            before_date, after_date = dates[labels.index(before_label)], dates[labels.index(after_label)]
            try:
                with st.status("변경 내역 조회", expanded=True) as status:
                    started = perf_counter(); before_layout, after_layout = fetch_layout_pair(target, schema_name, table_name, before_date, after_date); loaded = perf_counter()-started
                    tables, columns = compare_layouts(before_layout, after_layout)
                    status.update(label=f"완료 · {len(tables):,} 테이블 · {len(columns):,} 컬럼 · {loaded:.1f}초", state="complete", expanded=False)
                st.session_state.gp_layout_comparison = {"tables": tables, "columns": columns, "before": before_layout, "after": after_layout}
            except Exception as error:
                st.error(str(error), icon=":material/error:")
    if captured:
        try:
            standard_date = standard_day.strftime("%Y%m%d")
            with st.status("원천 레이아웃 적재", expanded=True) as status:
                layout = fetch_source_layout(source, selected_schemas, standard_date)
                count = save_layout(target, schema_name, table_name, standard_date, selected_schemas, layout)
                status.update(label=f"완료 · {count:,} 컬럼", state="complete", expanded=False)
            fetch_snapshot_dates.clear()
            st.session_state.gp_layout_capture = {"date": standard_date, "count": count}
            st.toast(f"{standard_date} 레이아웃을 적재했습니다.", icon=":material/check_circle:")
        except Exception as error:
            st.error(str(error), icon=":material/error:")
    render_comparison()


if __name__ == "__main__":
    main()

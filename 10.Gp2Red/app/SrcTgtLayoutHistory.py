from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import streamlit as st

from SrcTgtConnection import connection_frame, connection_label, runtime_connection_values, selectable_connections
from SrcTgtRuntime import connect, qualified, query_frame, runtime_context, text
from SrcTgtTargetReflection import render_target_reflection


def source_schemas(values: dict[str, Any]) -> list[str]:
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog') AND schema_name NOT LIKE 'pg_%' ORDER BY schema_name")
            return [text(row[0]) for row in cursor.fetchall()]


def source_layout(values: dict[str, Any], connection_id: str, standard_date: str, schemas: list[str]) -> pd.DataFrame:
    if not schemas:
        raise ValueError("원천 스키마를 한 개 이상 선택하십시오.")
    query = '''SELECT %s, %s, c.table_schema, c.table_name, COALESCE(obj_description(pc.oid, 'pg_class'), ''), c.ordinal_position, c.column_name, COALESCE(col_description(pc.oid, c.ordinal_position), ''), c.data_type, COALESCE(c.character_maximum_length::text, c.numeric_precision::text, ''), CASE WHEN pk.column_name IS NULL THEN FALSE ELSE TRUE END, CASE WHEN c.is_nullable = 'YES' THEN TRUE ELSE FALSE END
                 FROM information_schema.columns c
                 JOIN pg_namespace pn ON pn.nspname = c.table_schema
                 JOIN pg_class pc ON pc.relnamespace = pn.oid AND pc.relname = c.table_name
                 LEFT JOIN (
                     SELECT kcu.table_schema, kcu.table_name, kcu.column_name
                       FROM information_schema.table_constraints tc
                       JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema
                      WHERE tc.constraint_type = 'PRIMARY KEY'
                 ) pk ON pk.table_schema = c.table_schema AND pk.table_name = c.table_name AND pk.column_name = c.column_name
                WHERE c.table_schema = ANY(%s)
                ORDER BY c.table_schema, c.table_name, c.ordinal_position'''
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (connection_id, standard_date, schemas))
            rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=["SRC_CONN_ID", "STD_DT", "SRC_SCH_NM", "SRC_TBL_NM", "SRC_TBL_CMT", "SRC_COL_NO", "SRC_COL_NM", "SRC_COL_CMT", "SRC_DATA_TYPE", "SRC_DATA_LEN", "SRC_PK_YN", "SRC_NULL_YN"])


def save_layout(context_values: dict[str, Any], schema_name: str, layout: pd.DataFrame, connection_id: str, standard_date: str, schemas: list[str]) -> int:
    if layout.empty:
        raise ValueError("수집된 원천 레이아웃이 없습니다.")
    table_name = qualified(schema_name, "tb_mig_src_layout")
    with connect(context_values) as connection:
        with connection.cursor() as cursor:
            for schema in schemas:
                cursor.execute(f"DELETE FROM {table_name} WHERE src_conn_id = %s AND std_dt = %s AND src_sch_nm = %s", (connection_id, standard_date, schema))
            cursor.executemany(f"INSERT INTO {table_name} (src_conn_id, std_dt, src_sch_nm, src_tbl_nm, src_tbl_cmt, src_col_no, src_col_nm, src_col_cmt, src_data_type, src_data_len, src_pk_yn, src_null_yn) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", [tuple(row) for row in layout.itertuples(index=False, name=None)])
        connection.commit()
    return len(layout)


def dates(context_values: dict[str, Any], schema_name: str, connection_id: str) -> list[str]:
    frame = query_frame(context_values, f"SELECT DISTINCT std_dt FROM {qualified(schema_name, 'tb_mig_src_layout')} WHERE src_conn_id = %s ORDER BY std_dt", (connection_id,))
    return [text(value) for value in frame.std_dt.tolist()]


def comparison(context_values: dict[str, Any], schema_name: str, connection_id: str, before: str, after: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    query = f'''SELECT std_dt, src_sch_nm, src_tbl_nm, src_col_no, src_col_nm, src_data_type, src_data_len, src_pk_yn, src_null_yn
                  FROM {qualified(schema_name, 'tb_mig_src_layout')}
                 WHERE src_conn_id = %s AND std_dt IN (%s, %s)'''
    frame = query_frame(context_values, query, (connection_id, before, after))
    old = frame.loc[frame.std_dt.map(text).eq(before)].copy()
    new = frame.loc[frame.std_dt.map(text).eq(after)].copy()
    keys = ["src_sch_nm", "src_tbl_nm", "src_col_no"]
    merged = old.merge(new, on=keys, how="outer", suffixes=("_BF", "_AF"), indicator=True)
    fields = ["src_col_nm", "src_data_type", "src_data_len", "src_pk_yn", "src_null_yn"]
    changed = merged.loc[(merged._merge.ne("both")) | (merged.apply(lambda row: any(text(row[f"{field}_BF"]) != text(row[f"{field}_AF"]) for field in fields), axis=1))].copy()
    changed["CHG_DVSN"] = changed._merge.map({"left_only": "삭제", "right_only": "신규", "both": "변경"})
    tables = changed.groupby(["src_sch_nm", "src_tbl_nm", "CHG_DVSN"], dropna=False).size().reset_index(name="COL_CNT")
    return tables, changed


st.title("🧱 구조·변경")

try:
    context = runtime_context()
except Exception:
    st.info("초기 설정 메뉴에서 메타 연결과 스키마를 준비한 뒤 다시 선택하십시오.", icon=":material/settings:")
    st.stop()

mode = st.segmented_control("업무", ["원천 레이아웃", "변경 비교", "대상 반영안"], default="원천 레이아웃", label_visibility="collapsed")
if mode == "대상 반영안":
    render_target_reflection(context)
    st.stop()

try:
    connections = connection_frame(query_frame, context.values, context.schema_name, qualified, active_only=True)
    sources = selectable_connections(connections)
    sources = sources.loc[sources.dbms_cd.map(text).str.upper().eq("GREENPLUM")]
    if sources.empty:
        raise ValueError("Greenplum 원천 접속정보를 등록하십시오.")
    connection_id = st.selectbox("원천 접속", sources.conn_id.tolist(), format_func=lambda item: connection_label(sources, item))
except Exception as error:
    st.error(f"원천 접속 조회 실패: {error}", icon=":material/error:")
    st.stop()

if mode == "원천 레이아웃":
    try:
        source_values = runtime_connection_values(sources, connection_id)
        schemas = source_schemas(source_values)
    except Exception as error:
        st.error(f"원천 스키마 조회 실패: {error}", icon=":material/error:")
        st.stop()
    with st.form("layout_capture"):
        standard_day = st.date_input("기준일", value=date.today(), format="YYYY-MM-DD")
        selected = st.multiselect("원천 스키마", schemas)
        captured = st.form_submit_button("수집", type="primary", icon=":material/download:")
    if captured:
        try:
            standard = standard_day.strftime("%Y%m%d")
            layout = source_layout(source_values, connection_id, standard, selected)
            count = save_layout(context.values, context.schema_name, layout, connection_id, standard, selected)
            st.success(f"{count:,} 컬럼을 수집했습니다.", icon=":material/check_circle:")
        except Exception as error:
            st.error(f"원천 레이아웃 수집 실패: {error}", icon=":material/error:")
else:
    try:
        available = dates(context.values, context.schema_name, connection_id)
        if len(available) < 2:
            raise ValueError("비교할 기준일이 두 건 이상 필요합니다.")
        before, after = st.selectbox("이전 기준일", available, index=len(available) - 2), st.selectbox("비교 기준일", available, index=len(available) - 1)
        if st.button("비교", type="primary", icon=":material/compare_arrows:"):
            if before == after:
                raise ValueError("서로 다른 기준일을 선택하십시오.")
            tables, columns = comparison(context.values, context.schema_name, connection_id, before, after)
            st.dataframe(tables.rename(columns={"src_sch_nm": "원천스키마", "src_tbl_nm": "원천테이블", "CHG_DVSN": "변경구분", "COL_CNT": "변경컬럼수"}), hide_index=True)
            st.dataframe(columns.rename(columns={"src_sch_nm": "원천스키마", "src_tbl_nm": "원천테이블", "src_col_no": "원천컬럼순번", "CHG_DVSN": "변경구분"}), hide_index=True, height=420)
    except Exception as error:
        st.error(f"변경 비교 실패: {error}", icon=":material/error:")

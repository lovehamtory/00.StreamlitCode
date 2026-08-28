from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import streamlit as st

from SrcTgtConnection import connection_frame, connection_label, runtime_connection_values, selectable_connections
from SrcTgtRuntime import connect, qualified, query_frame, runtime_context, text


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


def captured_tables(values: dict[str, Any], schema_name: str, connection_id: str) -> pd.DataFrame:
    table_name = qualified(schema_name, "tb_mig_src_layout")
    query = f'''SELECT std_dt, src_sch_nm, src_tbl_nm, MAX(src_tbl_cmt) AS src_tbl_cmt, COUNT(*) AS src_col_cnt
                  FROM {table_name}
                 WHERE src_conn_id = %s
                   AND std_dt = (SELECT MAX(std_dt) FROM {table_name} WHERE src_conn_id = %s)
                 GROUP BY std_dt, src_sch_nm, src_tbl_nm
                 ORDER BY src_sch_nm, src_tbl_nm'''
    return query_frame(values, query, (connection_id, connection_id))


def mapped_tables(values: dict[str, Any], schema_name: str, connection_id: str, source_schema: str, source_table: str) -> pd.DataFrame:
    query = f'''SELECT T.mpg_id, T.sbj_area_cd, T.tgt_sch_nm, T.tgt_tbl_nm
                  FROM {qualified(schema_name, 'tb_mig_tbl_mpg')} T
                  JOIN {qualified(schema_name, 'tb_mig_sbj_area')} A ON A.sbj_area_cd = T.sbj_area_cd
                 WHERE T.active_yn = TRUE
                   AND A.src_conn_id = %s
                   AND T.src_sch_nm = %s
                   AND T.src_tbl_nm = %s
                 ORDER BY T.mpg_id'''
    return query_frame(values, query, (connection_id, source_schema, source_table))


st.subheader(":material/table_chart: 테이블 레이아웃")

try:
    context = runtime_context()
except Exception:
    st.info("초기 설정 메뉴에서 메타 연결과 스키마를 준비한 뒤 다시 선택하십시오.", icon=":material/settings:")
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

try:
    captured = captured_tables(context.values, context.schema_name, connection_id)
    if not captured.empty:
        selected_key = st.selectbox("수집 테이블", captured.index.tolist(), format_func=lambda index: f"{captured.loc[index].src_sch_nm}.{captured.loc[index].src_tbl_nm} · {int(captured.loc[index].src_col_cnt):,} 컬럼")
        selected_table = captured.loc[selected_key]
        mapped = mapped_tables(context.values, context.schema_name, connection_id, text(selected_table.src_sch_nm), text(selected_table.src_tbl_nm))
        if mapped.empty:
            if st.button("매핑 신규", icon=":material/link_add:"):
                st.switch_page("SrcTgtControl.py", query_params={"src_conn_id": connection_id, "src_std_dt": text(selected_table.std_dt), "src_sch_nm": text(selected_table.src_sch_nm), "src_tbl_nm": text(selected_table.src_tbl_nm)})
        else:
            mapping_id = st.selectbox("테이블 매핑", mapped.mpg_id.tolist(), format_func=lambda value: f"{value} · {mapped.loc[mapped.mpg_id.eq(value)].iloc[0].tgt_sch_nm}.{mapped.loc[mapped.mpg_id.eq(value)].iloc[0].tgt_tbl_nm}")
            if st.button("매핑 수정", icon=":material/edit:"):
                st.switch_page("SrcTgtControl.py", query_params={"mpg_id": str(int(mapping_id))})
except Exception as error:
    st.error(f"수집 테이블 조회 실패: {error}", icon=":material/error:")

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

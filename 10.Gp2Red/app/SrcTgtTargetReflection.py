from __future__ import annotations

import re
from typing import Any

import pandas as pd
import streamlit as st

from SrcTgtDataType import redshift_type
from SrcTgtRuntime import RuntimeContext, connect, qualified, query_frame, text


def table_maps(context: RuntimeContext) -> pd.DataFrame:
    query = f'''SELECT mpg_id, prj_cd, sbj_area_cd, src_conn_id, src_sch_nm, src_tbl_nm,
                       tgt_sch_nm, tgt_tbl_nm, tgt_dist_style, tgt_dist_key_col, tgt_sort_style, tgt_sort_cols, tgt_encd_auto_yn, tgt_ddl_sql
                  FROM {qualified(context.schema_name, "tb_mig_tbl_mpg")}
                 WHERE active_yn = TRUE
                 ORDER BY prj_cd, sbj_area_cd, tgt_sch_nm, tgt_tbl_nm, mpg_id'''
    return query_frame(context.values, query)


def column_maps(context: RuntimeContext, mapping_id: int) -> pd.DataFrame:
    query = f'''SELECT col_ord, src_col_no, src_col_nm, src_data_type, src_null_yn, src_key_role_cd,
                       tgt_col_no, tgt_col_nm, tgt_data_type, tgt_null_yn, tgt_key_role_cd, trnsf_expr, dflt_expr
                  FROM {qualified(context.schema_name, "tb_mig_col_mpg")}
                 WHERE mpg_id = %s AND active_yn = TRUE
                 ORDER BY col_ord'''
    return query_frame(context.values, query, (mapping_id,))


def layout_dates(values: dict[str, Any], schema_name: str, table_name: str, source_connection_id: str) -> list[str]:
    query = f"SELECT DISTINCT std_dt FROM {qualified(schema_name, table_name)} WHERE COALESCE(src_conn_id, 'SRC_GP') = %s ORDER BY std_dt DESC"
    return [text(row[0]) for row in query_frame(values, query, (source_connection_id,)).itertuples(index=False, name=None) if text(row[0])]


def source_layout(values: dict[str, Any], schema_name: str, table_name: str, source_connection_id: str, standard_date: str, owner: str, table: str) -> pd.DataFrame:
    query = f'''SELECT colno AS "원천 컬럼순번", col AS "원천 컬럼명", attr AS "원천 컬럼설명", datatype AS "원천 데이터타입", len AS "원천 길이", ispk AS "원천 PK", nullable AS "원천 NULL허용"
                  FROM {qualified(schema_name, table_name)}
                 WHERE COALESCE(src_conn_id, 'SRC_GP') = %s
                   AND std_dt = %s
                   AND UPPER(owner) = UPPER(%s)
                   AND UPPER(tbl) = UPPER(%s)
                 ORDER BY colno'''
    return query_frame(values, query, (source_connection_id, standard_date, owner, table))


def target_columns(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.rename(columns={
        "col_ord": "매핑순서", "src_col_no": "매핑 원천순번", "src_col_nm": "매핑 원천컬럼", "src_data_type": "매핑 원천타입", "src_null_yn": "매핑 원천NULL", "src_key_role_cd": "매핑 원천키",
        "tgt_col_no": "대상 컬럼순번", "tgt_col_nm": "대상 컬럼명", "tgt_data_type": "대상 데이터타입", "tgt_null_yn": "대상 NULL허용", "tgt_key_role_cd": "대상 키", "trnsf_expr": "변환식", "dflt_expr": "기본값식",
    })


def identifier(value: object) -> str:
    name = text(value)
    if not name or "\x00" in name or len(name.encode("utf-8")) > 127:
        raise ValueError("대상 식별자 형식이 올바르지 않습니다.")
    return '"' + name.replace('"', '""') + '"'


def target_type(value: object) -> str:
    return redshift_type(value)


def ddl_for(table: pd.Series, columns: pd.DataFrame) -> str:
    if columns.empty:
        raise ValueError("DDL을 생성할 컬럼 매핑이 없습니다.")
    definitions: list[str] = []
    for row in columns.itertuples(index=False):
        nullable = "" if bool(row.tgt_null_yn) else " NOT NULL"
        default = f" DEFAULT {text(row.dflt_expr)}" if text(row.dflt_expr) else ""
        definitions.append(f"    {identifier(row.tgt_col_nm)} {target_type(row.tgt_data_type)}{default}{nullable}")
    distribution = text(table.tgt_dist_style).upper() or "AUTO"
    if distribution not in {"AUTO", "EVEN", "KEY", "ALL"}:
        raise ValueError("대상 분산 방식을 확인하십시오.")
    clauses = [f"DISTSTYLE {distribution}"]
    if distribution == "KEY":
        if not text(table.tgt_dist_key_col):
            raise ValueError("분산 방식 KEY에는 대상 분산키가 필요합니다.")
        clauses.append(f"DISTKEY ({identifier(table.tgt_dist_key_col)})")
    sort_style = text(table.tgt_sort_style).upper() or "AUTO"
    sort_keys = [text(value) for value in text(table.tgt_sort_cols).split(",") if text(value)]
    if sort_style == "AUTO":
        clauses.append("SORTKEY AUTO")
    elif sort_style in {"COMPOUND", "INTERLEAVED"} and sort_keys:
        clauses.append(f"{sort_style} SORTKEY ({', '.join(identifier(value) for value in sort_keys)})")
    elif sort_style not in {"", "NONE"}:
        raise ValueError("대상 정렬 방식을 확인하십시오.")
    if bool(table.tgt_encd_auto_yn):
        clauses.append("ENCODE AUTO")
    return f"CREATE TABLE IF NOT EXISTS {qualified(text(table.tgt_sch_nm), text(table.tgt_tbl_nm))} (\n{',\n'.join(definitions)}\n)\n{'\n'.join(clauses)};"


def save_target_design(context: RuntimeContext, mapping_id: int, design: dict[str, object]) -> None:
    query = f'''UPDATE {qualified(context.schema_name, "tb_mig_tbl_mpg")}
                   SET tgt_dist_style = %s, tgt_dist_key_col = %s, tgt_sort_style = %s, tgt_sort_cols = %s, tgt_encd_auto_yn = %s,
                       tgt_ddl_sql = NULL, meta_ver_no = meta_ver_no + 1, upd_dtm = GETDATE()
                 WHERE mpg_id = %s'''
    with connect(context.values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (design["tgt_dist_style"], design["tgt_dist_key_col"] or None, design["tgt_sort_style"], design["tgt_sort_cols"] or None, design["tgt_encd_auto_yn"], mapping_id))
        connection.commit()


def save_ddl(context: RuntimeContext, mapping_id: int, ddl: str) -> None:
    query = f"UPDATE {qualified(context.schema_name, 'tb_mig_tbl_mpg')} SET tgt_ddl_sql = %s, meta_ver_no = meta_ver_no + 1, upd_dtm = GETDATE() WHERE mpg_id = %s"
    with connect(context.values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (ddl, mapping_id))
        connection.commit()


def mapping_label(frame: pd.DataFrame, mapping_id: int) -> str:
    row = frame.loc[frame.mpg_id.eq(mapping_id)].iloc[0]
    return f"{int(mapping_id)} · {row.src_sch_nm}.{row.src_tbl_nm} → {row.tgt_sch_nm}.{row.tgt_tbl_nm}"


def render_target_reflection(context: RuntimeContext, layout_values: dict[str, Any], layout_schema: str, layout_table: str) -> None:
    st.subheader("대상 반영안")
    try:
        mappings = table_maps(context)
    except Exception as error:
        st.error(f"매핑을 조회할 수 없습니다: {error}", icon=":material/error:")
        return
    if mappings.empty:
        st.info("대상 반영안을 만들 테이블 매핑이 없습니다.", icon=":material/info:")
        return
    mapping_id = st.selectbox("테이블 매핑", mappings.mpg_id.tolist(), format_func=lambda value: mapping_label(mappings, value), key="target_reflection_mapping")
    mapping = mappings.loc[mappings.mpg_id.eq(mapping_id)].iloc[0]
    try:
        dates = layout_dates(layout_values, layout_schema, layout_table, text(mapping.src_conn_id))
    except Exception as error:
        st.error(f"원천 기준일을 조회할 수 없습니다: {error}", icon=":material/error:")
        return
    selected_date = st.selectbox("원천 기준일", dates, key="target_reflection_date") if dates else None
    source, target = st.columns(2)
    with source:
        st.markdown("#### 원천 구조")
        if selected_date:
            try:
                source_frame = source_layout(layout_values, layout_schema, layout_table, text(mapping.src_conn_id), text(selected_date), text(mapping.src_sch_nm), text(mapping.src_tbl_nm))
                if source_frame.empty:
                    st.info("선택 기준일의 원천 레이아웃이 없습니다.", icon=":material/info:")
                else:
                    st.dataframe(source_frame, hide_index=True, height=360)
            except Exception as error:
                st.error(f"원천 구조 조회 실패: {error}", icon=":material/error:")
        else:
            st.info("원천 레이아웃 기준일을 먼저 수집하십시오.", icon=":material/info:")
    with target:
        st.markdown("#### 대상 반영안")
        columns = column_maps(context, int(mapping_id))
        st.dataframe(target_columns(columns), hide_index=True, height=360)
    can_edit = True
    with st.form("target_design_form"):
        left, right = st.columns(2)
        with left:
            dist_options = ["AUTO", "EVEN", "KEY", "ALL"]
            current_dist = text(mapping.tgt_dist_style).upper() or "AUTO"
            tgt_dist_style = st.selectbox("대상 분산 방식", dist_options, index=dist_options.index(current_dist) if current_dist in dist_options else 0)
            tgt_dist_key_col = st.text_input("대상 분산키", value=text(mapping.tgt_dist_key_col))
        with right:
            sort_options = ["AUTO", "NONE", "COMPOUND", "INTERLEAVED"]
            current_sort = text(mapping.tgt_sort_style).upper() or "AUTO"
            tgt_sort_style = st.selectbox("대상 정렬 방식", sort_options, index=sort_options.index(current_sort) if current_sort in sort_options else 0)
            tgt_sort_cols = st.text_input("대상 정렬키", value=text(mapping.tgt_sort_cols))
            tgt_encd_auto_yn = st.toggle("대상 자동 압축", value=False if pd.isna(mapping.tgt_encd_auto_yn) else bool(mapping.tgt_encd_auto_yn))
        saved = st.form_submit_button("대상 반영안 저장", type="primary", icon=":material/save:", disabled=not can_edit)
    if saved:
        try:
            if tgt_dist_style == "KEY" and not text(tgt_dist_key_col):
                raise ValueError("대상 분산 방식 KEY에는 대상 분산키가 필요합니다.")
            if tgt_sort_style in {"COMPOUND", "INTERLEAVED"} and not text(tgt_sort_cols):
                raise ValueError("대상 정렬 방식에는 대상 정렬키가 필요합니다.")
            save_target_design(context, int(mapping_id), {"tgt_dist_style": tgt_dist_style, "tgt_dist_key_col": text(tgt_dist_key_col), "tgt_sort_style": tgt_sort_style, "tgt_sort_cols": text(tgt_sort_cols), "tgt_encd_auto_yn": tgt_encd_auto_yn})
            st.rerun()
        except Exception as error:
            st.error(str(error), icon=":material/error:")
    try:
        current_mapping = table_maps(context).loc[lambda frame: frame.mpg_id.eq(mapping_id)].iloc[0]
        ddl = ddl_for(current_mapping, column_maps(context, int(mapping_id)))
        st.markdown("#### 대상 DDL")
        st.code(ddl, language="sql")
        with st.container(horizontal=True, horizontal_alignment="left"):
            if st.button("DDL 저장", icon=":material/save:", disabled=not can_edit, key="target_reflection_ddl_save"):
                save_ddl(context, int(mapping_id), ddl)
                st.rerun()
            st.download_button("DDL 다운로드", ddl, file_name=f"{text(mapping.tgt_sch_nm)}_{text(mapping.tgt_tbl_nm)}_reference.sql", mime="text/sql", icon=":material/download:")
    except Exception as error:
        st.error(str(error), icon=":material/error:")

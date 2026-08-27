from __future__ import annotations

import re
from typing import Any

import pandas as pd
import streamlit as st

from SrcTgtConnection import connection_frame, runtime_connection_values
from SrcTgtDataType import redshift_type
from SrcTgtRuntime import RuntimeContext, connect, qualified, query_frame, text


def table_maps(context: RuntimeContext) -> pd.DataFrame:
    query = f'''SELECT mpg_id, prj_cd, sbj_area_cd, src_conn_id, src_sch_nm, src_tbl_nm, tgt_conn_id, tgt_sch_nm, tgt_tbl_nm, tgt_tbl_cmt, tgt_ddl_sql
                  FROM {qualified(context.schema_name, "tb_mig_tbl_mpg")}
                 WHERE active_yn = TRUE
                 ORDER BY prj_cd, sbj_area_cd, tgt_sch_nm, tgt_tbl_nm, mpg_id'''
    return query_frame(context.values, query)


def column_maps(context: RuntimeContext, mapping_id: int) -> pd.DataFrame:
    query = f'''SELECT col_ord, src_col_no, src_col_nm, src_data_type, src_null_yn, src_key_role_cd,
                       tgt_col_no, tgt_col_nm, tgt_col_cmt, tgt_data_type, tgt_null_yn, tgt_key_role_cd, tgt_expr, dflt_expr
                  FROM {qualified(context.schema_name, "tb_mig_col_mpg")}
                 WHERE mpg_id = %s AND active_yn = TRUE
                 ORDER BY col_ord'''
    return query_frame(context.values, query, (mapping_id,))


def layout_dates(values: dict[str, Any], schema_name: str, source_connection_id: str) -> list[str]:
    query = f"SELECT DISTINCT std_dt FROM {qualified(schema_name, 'tb_mig_src_layout')} WHERE src_conn_id = %s ORDER BY std_dt DESC"
    return [text(row[0]) for row in query_frame(values, query, (source_connection_id,)).itertuples(index=False, name=None) if text(row[0])]


def source_layout(values: dict[str, Any], schema_name: str, source_connection_id: str, standard_date: str, owner: str, table: str) -> pd.DataFrame:
    query = f'''SELECT src_col_no AS "원천 컬럼순번", src_col_nm AS "원천 컬럼명", src_col_cmt AS "원천 컬럼설명", src_data_type AS "원천 데이터타입", src_data_len AS "원천 길이", CASE WHEN src_pk_yn THEN 'Y' ELSE '' END AS "원천 PK", CASE WHEN src_null_yn THEN 'Y' ELSE 'N' END AS "원천 NULL허용"
                  FROM {qualified(schema_name, 'tb_mig_src_layout')}
                 WHERE src_conn_id = %s AND std_dt = %s AND UPPER(src_sch_nm) = UPPER(%s) AND UPPER(src_tbl_nm) = UPPER(%s)
                 ORDER BY src_col_no'''
    return query_frame(values, query, (source_connection_id, standard_date, owner, table))


def target_columns(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.rename(columns={
        "col_ord": "매핑순서", "src_col_no": "매핑 원천순번", "src_col_nm": "매핑 원천컬럼", "src_data_type": "매핑 원천타입", "src_null_yn": "매핑 원천NULL", "src_key_role_cd": "매핑 원천키",
        "tgt_col_no": "대상 컬럼순번", "tgt_col_nm": "대상 컬럼명", "tgt_col_cmt": "대상 컬럼설명", "tgt_data_type": "대상 데이터타입", "tgt_null_yn": "대상 NULL허용", "tgt_key_role_cd": "대상 키", "tgt_expr": "이행 적용SQL식", "dflt_expr": "이행 기본값식",
    })


def identifier(value: object) -> str:
    name = text(value)
    if not name or "\x00" in name or len(name.encode("utf-8")) > 127:
        raise ValueError("대상 식별자 형식이 올바르지 않습니다.")
    return '"' + name.replace('"', '""') + '"'


def literal(value: object) -> str:
    return "'" + text(value).replace("'", "''") + "'"


def parse_design(ddl: object) -> dict[str, object]:
    source = text(ddl).upper()
    dist = re.search(r"\bDISTSTYLE\s+(AUTO|EVEN|KEY|ALL)\b", source)
    dist_key = re.search(r"\bDISTKEY\s*\(\s*\"?([^\")\s]+)", text(ddl), re.IGNORECASE)
    sort = re.search(r"\b(COMPOUND|INTERLEAVED)\s+SORTKEY\s*\(([^)]*)\)", text(ddl), re.IGNORECASE | re.DOTALL)
    auto_sort = bool(re.search(r"\bSORTKEY\s+AUTO\b", source))
    return {
        "dist_style": dist.group(1) if dist else "AUTO",
        "dist_key": text(dist_key.group(1)) if dist_key else "",
        "sort_style": sort.group(1).upper() if sort else "AUTO" if auto_sort else "NONE",
        "sort_cols": text(sort.group(2)) if sort else "",
        "encd_auto": bool(re.search(r"\bENCODE\s+AUTO\b", source)),
    }


def ddl_for(table: pd.Series, columns: pd.DataFrame, design: dict[str, object]) -> str:
    if columns.empty:
        raise ValueError("DDL을 생성할 컬럼 매핑이 없습니다.")
    definitions: list[str] = []
    for row in columns.itertuples(index=False):
        nullable = "" if bool(row.tgt_null_yn) else " NOT NULL"
        default = f" DEFAULT {text(row.dflt_expr)}" if text(row.dflt_expr) else ""
        definitions.append(f"    {identifier(row.tgt_col_nm)} {redshift_type(row.tgt_data_type)}{default}{nullable}")
    dist_style = text(design["dist_style"]).upper()
    sort_style = text(design["sort_style"]).upper()
    if dist_style not in {"AUTO", "EVEN", "KEY", "ALL"}:
        raise ValueError("대상 분산 방식을 선택하십시오.")
    if sort_style not in {"AUTO", "NONE", "COMPOUND", "INTERLEAVED"}:
        raise ValueError("대상 정렬 방식을 선택하십시오.")
    clauses = [f"DISTSTYLE {dist_style}"]
    if dist_style == "KEY":
        if not text(design["dist_key"]):
            raise ValueError("KEY 분산 방식에는 분산키가 필요합니다.")
        clauses.append(f"DISTKEY ({identifier(design['dist_key'])})")
    sort_columns = [text(value).strip('"') for value in text(design["sort_cols"]).split(",") if text(value)]
    if sort_style == "AUTO":
        clauses.append("SORTKEY AUTO")
    elif sort_style in {"COMPOUND", "INTERLEAVED"}:
        if not sort_columns:
            raise ValueError("대상 정렬 방식에는 정렬키가 필요합니다.")
        clauses.append(f"{sort_style} SORTKEY ({', '.join(identifier(value) for value in sort_columns)})")
    if bool(design["encd_auto"]):
        clauses.append("ENCODE AUTO")
    target = qualified(text(table.tgt_sch_nm), text(table.tgt_tbl_nm))
    statements = [f"DROP TABLE IF EXISTS {target}", f"CREATE TABLE {target} (\n{',\n'.join(definitions)}\n)\n" + "\n".join(clauses)]
    if text(table.tgt_tbl_cmt):
        statements.append(f"COMMENT ON TABLE {target} IS {literal(table.tgt_tbl_cmt)}")
    for row in columns.itertuples(index=False):
        if text(row.tgt_col_cmt):
            statements.append(f"COMMENT ON COLUMN {target}.{identifier(row.tgt_col_nm)} IS {literal(row.tgt_col_cmt)}")
    return ";\n\n".join(statements) + ";"


def show_table_ddl(values: dict[str, Any], schema_name: str, table_name: str) -> str:
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SHOW TABLE {qualified(schema_name, table_name)}")
            return "\n".join("".join(text(value) for value in row) for row in cursor.fetchall())


def save_ddl(context: RuntimeContext, mapping_id: int, ddl: str) -> None:
    query = f"UPDATE {qualified(context.schema_name, 'tb_mig_tbl_mpg')} SET tgt_ddl_sql = %s, meta_ver_no = meta_ver_no + 1, upd_dtm = GETDATE() WHERE mpg_id = %s"
    with connect(context.values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (ddl, mapping_id))
        connection.commit()


def apply_ddl(values: dict[str, Any], ddl: str) -> None:
    commands = [statement.strip() for statement in ddl.split(";") if statement.strip()]
    if len(commands) < 2 or not commands[0].upper().startswith("DROP TABLE IF EXISTS ") or not commands[1].upper().startswith("CREATE TABLE ") or any(not command.upper().startswith("COMMENT ON ") for command in commands[2:]):
        raise ValueError("실행 DDL은 DROP TABLE IF EXISTS, CREATE TABLE, 선택 COMMENT ON 문장만 허용합니다.")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            for command in commands:
                cursor.execute(command)
        connection.commit()


def mapping_label(frame: pd.DataFrame, mapping_id: int) -> str:
    row = frame.loc[frame.mpg_id.eq(mapping_id)].iloc[0]
    return f"{int(mapping_id)} · {row.src_sch_nm}.{row.src_tbl_nm} → {row.tgt_sch_nm}.{row.tgt_tbl_nm}"


def render_target_reflection(context: RuntimeContext) -> None:
    st.subheader("대상 반영안")
    try:
        mappings = table_maps(context)
        connections = connection_frame(query_frame, context.values, context.schema_name, qualified, active_only=True)
    except Exception as error:
        st.error(f"매핑을 조회할 수 없습니다: {error}", icon=":material/error:")
        return
    if mappings.empty:
        st.info("대상 반영안을 만들 테이블 매핑이 없습니다.", icon=":material/info:")
        return
    mapping_id = st.selectbox("테이블 매핑", mappings.mpg_id.tolist(), format_func=lambda value: mapping_label(mappings, value), key="target_reflection_mapping")
    mapping = mappings.loc[mappings.mpg_id.eq(mapping_id)].iloc[0]
    columns = column_maps(context, int(mapping_id))
    stored_key = f"target_ddl_source_{mapping_id}"
    if st.button("대상 DDL 조회", icon=":material/search:"):
        try:
            target_values = runtime_connection_values(connections, text(mapping.tgt_conn_id))
            st.session_state[stored_key] = show_table_ddl(target_values, text(mapping.tgt_sch_nm), text(mapping.tgt_tbl_nm))
        except Exception as error:
            st.error(f"대상 DDL 조회 실패: {error}", icon=":material/error:")
    current_ddl = text(st.session_state.get(stored_key)) or text(mapping.tgt_ddl_sql)
    design = parse_design(current_ddl)
    with st.expander("대상 구조", expanded=True):
        st.dataframe(target_columns(columns), hide_index=True, height=320)
    with st.form(f"target_ddl_form_{mapping_id}"):
        left, right = st.columns(2)
        with left:
            dist_options = ["AUTO", "EVEN", "KEY", "ALL"]
            dist_style = st.selectbox("분산 방식", dist_options, index=dist_options.index(text(design["dist_style"]).upper()) if text(design["dist_style"]).upper() in dist_options else 0)
            dist_key = st.text_input("분산키", value=text(design["dist_key"]))
        with right:
            sort_options = ["AUTO", "NONE", "COMPOUND", "INTERLEAVED"]
            sort_style = st.selectbox("정렬 방식", sort_options, index=sort_options.index(text(design["sort_style"]).upper()) if text(design["sort_style"]).upper() in sort_options else 0)
            sort_cols = st.text_input("정렬키", value=text(design["sort_cols"]))
            encd_auto = st.toggle("자동 압축", value=bool(design["encd_auto"]))
        ddl = ddl_for(mapping, columns, {"dist_style": dist_style, "dist_key": dist_key, "sort_style": sort_style, "sort_cols": sort_cols, "encd_auto": encd_auto})
        executable_ddl = st.text_area("실행 DDL", value=ddl, height=320)
        confirmed = st.checkbox("DROP TABLE 실행 확인")
        previewed = st.form_submit_button("DDL 저장", icon=":material/save:")
        applied = st.form_submit_button("대상 적용", type="primary", icon=":material/play_arrow:")
    if previewed:
        try:
            save_ddl(context, int(mapping_id), executable_ddl)
            st.success("DDL을 저장했습니다.", icon=":material/check_circle:")
        except Exception as error:
            st.error(f"DDL 저장 실패: {error}", icon=":material/error:")
    if applied:
        try:
            if not confirmed:
                raise ValueError("DROP TABLE 실행 확인을 선택하십시오.")
            target_values = runtime_connection_values(connections, text(mapping.tgt_conn_id))
            apply_ddl(target_values, executable_ddl)
            save_ddl(context, int(mapping_id), executable_ddl)
            st.success("대상 DDL을 적용했습니다.", icon=":material/check_circle:")
        except Exception as error:
            st.error(f"대상 적용 실패: {error}", icon=":material/error:")

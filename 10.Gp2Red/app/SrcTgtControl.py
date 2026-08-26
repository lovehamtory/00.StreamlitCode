from __future__ import annotations

import re
import sys
from typing import Any

import pandas as pd
import streamlit as st

from SrcTgtArtifact import render_artifacts
from SrcTgtConnection import render_connection_management
from SrcTgtDagGenerator import render_dag_generator, render_project_dag_generator
from SrcTgtMapping import render_mapping_workspace
from SrcTgtRuntime import runtime_context
from SrcTgtValidation import render_validation

try:
    import psycopg
except ImportError:
    psycopg = None


TABLE_COLUMNS = [
    "mpg_id", "prj_cd", "sbj_area_cd", "src_conn_id", "src_sch_nm", "src_tbl_nm", "tgt_conn_id", "tgt_sch_nm", "tgt_tbl_nm",
    "tgt_dist_style", "tgt_dist_key_col", "tgt_sort_style", "tgt_sort_cols", "tgt_encd_auto_yn", "load_sts_cd", "incr_basis_cd",
    "incr_basis_col_nm", "parl_mthd_cd", "parl_cnd_arr", "meta_ver_no",
]


def text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def identifier(value: str) -> str:
    name = text(value)
    if not name or "\x00" in name or len(name.encode("utf-8")) > 127:
        raise ValueError(f"식별자 형식이 올바르지 않습니다: {value}")
    return '"' + name.replace('"', '""') + '"'


def qualified(schema_name: str, table_name: str) -> str:
    return f"{identifier(schema_name)}.{identifier(table_name)}"


def connect(values: dict[str, Any]) -> Any:
    if psycopg is None:
        raise RuntimeError(f"psycopg가 현재 실행 Python에 설치되지 않았습니다: {sys.executable}")
    arguments: dict[str, Any] = {
        "host": text(values["host"]), "port": int(values["port"]), "dbname": text(values["database"]),
        "user": text(values["user"]), "password": text(values["password"]), "connect_timeout": int(values.get("connect_timeout", 15)),
    }
    if text(values.get("sslmode")):
        arguments["sslmode"] = text(values["sslmode"])
    return psycopg.connect(**arguments)


def query_frame(values: dict[str, Any], query: str, parameters: tuple[Any, ...] = ()) -> pd.DataFrame:
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
            return pd.DataFrame(cursor.fetchall(), columns=[column.name for column in cursor.description])


def subject_areas(values: dict[str, Any], schema_name: str) -> pd.DataFrame:
    query = f"""
        SELECT S.sbj_area_cd, S.sbj_area_nm, S.up_sbj_area_cd, P.sbj_area_nm AS up_sbj_area_nm, S.disp_ord, S.active_yn,
               COALESCE(X.pre_sbj_area_cds, '') AS pre_sbj_area_cds,
               MAX(CASE WHEN D.dag_dvsn_cd = 'S3' THEN D.dag_id END) AS s3_dag_id,
               MAX(CASE WHEN D.dag_dvsn_cd = 'S3' THEN D.dflt_parl_cnt END) AS s3_dflt_parl_cnt,
               MAX(CASE WHEN D.dag_dvsn_cd = 'S3' THEN D.max_parl_cnt END) AS s3_max_parl_cnt,
               MAX(CASE WHEN D.dag_dvsn_cd = 'INS' THEN D.dag_id END) AS ins_dag_id,
               MAX(CASE WHEN D.dag_dvsn_cd = 'INS' THEN D.dflt_parl_cnt END) AS ins_dflt_parl_cnt,
               MAX(CASE WHEN D.dag_dvsn_cd = 'INS' THEN D.max_parl_cnt END) AS ins_max_parl_cnt,
               MAX(CASE WHEN D.dag_dvsn_cd = 'FULL_CTL' THEN D.dag_id END) AS full_ctl_dag_id,
               MAX(CASE WHEN D.dag_dvsn_cd = 'INCR_CTL' THEN D.dag_id END) AS incr_ctl_dag_id,
               MAX(CASE WHEN D.dag_dvsn_cd = 'INCR_CTL' THEN D.schd_cd END) AS incr_schd_cd
          FROM {qualified(schema_name, 'tb_mig_sbj_area')} S
          LEFT JOIN {qualified(schema_name, 'tb_mig_sbj_area')} P ON P.sbj_area_cd = S.up_sbj_area_cd
          LEFT JOIN (
              SELECT nxt_sbj_area_cd, LISTAGG(pre_sbj_area_cd, ',') WITHIN GROUP (ORDER BY pre_sbj_area_cd) AS pre_sbj_area_cds
                FROM {qualified(schema_name, 'tb_mig_sbj_dep')}
               WHERE active_yn = TRUE
               GROUP BY nxt_sbj_area_cd
          ) X ON X.nxt_sbj_area_cd = S.sbj_area_cd
          LEFT JOIN {qualified(schema_name, 'tb_mig_sbj_dag_mpg')} D ON D.sbj_area_cd = S.sbj_area_cd
         GROUP BY S.sbj_area_cd, S.sbj_area_nm, S.up_sbj_area_cd, P.sbj_area_nm, S.disp_ord, S.active_yn, X.pre_sbj_area_cds
         ORDER BY S.disp_ord, S.sbj_area_cd
    """
    return query_frame(values, query)


def subject_code(value: object, label: str) -> str:
    code = text(value).upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,7}", code):
        raise ValueError(f"{label}는 영문으로 시작하는 영문·숫자·밑줄 1~8자리여야 합니다.")
    return code


def validates_subject_dependencies(cursor: Any, schema_name: str, code: str, predecessors: list[str]) -> None:
    if code in predecessors:
        raise ValueError("자기 자신을 선행 주제영역으로 지정할 수 없습니다.")
    cursor.execute(f"SELECT pre_sbj_area_cd, nxt_sbj_area_cd FROM {qualified(schema_name, 'tb_mig_sbj_dep')} WHERE active_yn = TRUE")
    rows = cursor.fetchall()
    graph: dict[str, set[str]] = {}
    for pre_code, nxt_code in rows:
        graph.setdefault(text(pre_code), set()).add(text(nxt_code))
    for pre_code in predecessors:
        stack = [code]
        visited: set[str] = set()
        while stack:
            current = stack.pop()
            if current == pre_code:
                raise ValueError("주제영역 선후행이 순환합니다.")
            if current in visited:
                continue
            visited.add(current)
            stack.extend(graph.get(current, set()))


def save_subject_area(values: dict[str, Any], schema_name: str, area: dict[str, Any]) -> None:
    old_code = subject_code(area["old_code"], "기존 주제영역 코드")
    code = subject_code(area["code"], "주제영역 코드")
    parent = "" if area["is_parent"] else subject_code(area["parent"], "상위 주제영역 코드")
    predecessors = sorted({subject_code(value, "선행 주제영역 코드") for value in area["predecessors"]})
    if code == parent:
        raise ValueError("주제영역 코드와 상위 주제영역 코드는 같을 수 없습니다.")
    subject_table = qualified(schema_name, "tb_mig_sbj_area")
    dependency_table = qualified(schema_name, "tb_mig_sbj_dep")
    dag_table = qualified(schema_name, "tb_mig_sbj_dag_mpg")
    mapping_table = qualified(schema_name, "tb_mig_tbl_mpg")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM {subject_table} WHERE sbj_area_cd = %s AND sbj_area_cd <> %s", (code, old_code))
            if cursor.fetchone() is not None:
                raise ValueError("이미 사용 중인 주제영역 코드입니다.")
            if parent:
                cursor.execute(f"SELECT 1 FROM {subject_table} WHERE sbj_area_cd = %s AND up_sbj_area_cd IS NULL", (parent,))
                if cursor.fetchone() is None:
                    raise ValueError("사용 가능한 상위 주제영역 코드를 선택하십시오.")
            if predecessors:
                cursor.execute(f"SELECT sbj_area_cd FROM {subject_table} WHERE sbj_area_cd = ANY(%s) AND up_sbj_area_cd IS NOT NULL AND active_yn = TRUE", (predecessors,))
                if {text(row[0]) for row in cursor.fetchall()} != set(predecessors):
                    raise ValueError("선행 주제영역은 사용 중인 실행 주제영역만 선택할 수 있습니다.")
            validates_subject_dependencies(cursor, schema_name, old_code, predecessors)
            cursor.execute(f"UPDATE {subject_table} SET sbj_area_cd = %s, sbj_area_nm = %s, up_sbj_area_cd = %s, disp_ord = %s, active_yn = %s, upd_dtm = GETDATE() WHERE sbj_area_cd = %s", (code, text(area["name"]) or None, parent or None, int(area["display_order"]), bool(area["active"]), old_code))
            if cursor.rowcount != 1:
                raise ValueError("수정할 주제영역을 찾을 수 없습니다.")
            if code != old_code:
                cursor.execute(f"UPDATE {subject_table} SET up_sbj_area_cd = %s, upd_dtm = GETDATE() WHERE up_sbj_area_cd = %s", (code, old_code))
                cursor.execute(f"UPDATE {dag_table} SET sbj_area_cd = %s, upd_dtm = GETDATE() WHERE sbj_area_cd = %s", (code, old_code))
                cursor.execute(f"UPDATE {mapping_table} SET sbj_area_cd = %s, upd_dtm = GETDATE() WHERE sbj_area_cd = %s", (code, old_code))
                cursor.execute(f"UPDATE {dependency_table} SET pre_sbj_area_cd = %s, upd_dtm = GETDATE() WHERE pre_sbj_area_cd = %s", (code, old_code))
                cursor.execute(f"UPDATE {dependency_table} SET nxt_sbj_area_cd = %s, upd_dtm = GETDATE() WHERE nxt_sbj_area_cd = %s", (code, old_code))
            cursor.execute(f"DELETE FROM {dependency_table} WHERE nxt_sbj_area_cd = %s", (code,))
            for pre_code in predecessors:
                cursor.execute(f"INSERT INTO {dependency_table} (pre_sbj_area_cd, nxt_sbj_area_cd, active_yn) VALUES (%s, %s, TRUE)", (pre_code, code))
        connection.commit()


def table_maps(values: dict[str, Any], schema_name: str) -> pd.DataFrame:
    columns = ", ".join(identifier(column) for column in TABLE_COLUMNS)
    query = f"SELECT {columns} FROM {qualified(schema_name, 'tb_mig_tbl_mpg')} WHERE active_yn = TRUE ORDER BY prj_cd, sbj_area_cd, mpg_id"
    return query_frame(values, query)


def run_logs(values: dict[str, Any], schema_name: str) -> pd.DataFrame:
    query = f"""
        SELECT wrk_dt, dag_nm, dag_run_id, task_nm, mpg_id, manf_id, meta_ver_no, s3_manf_path, load_mthd_cd, ins_scope_cd, sql_file_path, log_file_path, wrk_cnd_val,
               wrk_step_cd, wrk_sts_cd, src_row_cnt, tgt_row_cnt, src_size_byte, tgt_size_byte, wrk_stt_dtm, wrk_end_dtm, wrk_elps_sec, wrk_msg
          FROM {qualified(schema_name, 'tb_mig_run_log')}
         ORDER BY run_hist_id DESC
         LIMIT 500
    """
    return query_frame(values, query)


context = runtime_context()
target = context.values
metadata_schema = context.schema_name
try:
    maps = table_maps(target, metadata_schema)
except Exception as error:
    st.error(f"메타데이터 조회 실패: {error}", icon=":material/error:")
    st.stop()

st.title("🧭 이관 관리")
st.caption("⚙️ Created by ♡홍율파파")
view_options = ["🗂️ 주제영역", "⚙️ DAG 생성", "🔗 테이블·컬럼 매핑", "✅ 검증", "📋 실행 이력", "📦 산출물", "🔌 접속정보"]
view = st.segmented_control("업무", view_options, default="🗂️ 주제영역", label_visibility="collapsed")

if view == "🗂️ 주제영역":
    st.subheader("🗂️ 주제영역")
    if st.session_state.pop("subject_area_code_renamed", False):
        st.warning("코드 변경 후에는 새 DAG 파일을 생성하고 기존 Airflow DAG 파일을 별도로 정리하십시오.", icon=":material/warning:")
    areas = subject_areas(target, metadata_schema)
    st.dataframe(areas.rename(columns={"sbj_area_cd": "주제영역 코드", "sbj_area_nm": "주제영역명", "up_sbj_area_cd": "상위 주제영역 코드", "up_sbj_area_nm": "상위 주제영역명", "disp_ord": "표시 순서", "pre_sbj_area_cds": "선행 주제영역", "active_yn": "사용", "s3_dag_id": "S3 DAG", "ins_dag_id": "INS DAG", "full_ctl_dag_id": "전체 제어 DAG", "incr_ctl_dag_id": "증분 제어 DAG"}), hide_index=True)
    selected_area = st.selectbox("주제영역", areas.sbj_area_cd.tolist())
    current = areas.loc[areas.sbj_area_cd.eq(selected_area)].iloc[0]
    is_parent = not text(current.up_sbj_area_cd)
    parent_codes = areas.loc[areas.up_sbj_area_cd.map(text).eq(""), "sbj_area_cd"].tolist()
    execution_codes = areas.loc[areas.up_sbj_area_cd.map(text).ne(""), "sbj_area_cd"].tolist()
    selected_predecessors = [code for code in text(current.pre_sbj_area_cds).split(",") if code]
    with st.form("subject_area_form"):
        code = st.text_input("주제영역 코드", value=text(current.sbj_area_cd))
        name = st.text_input("주제영역명", value=text(current.sbj_area_nm))
        if is_parent:
            parent = ""
            st.text_input("상위 주제영역 코드", value="", disabled=True)
        else:
            parent = st.selectbox("상위 주제영역 코드", parent_codes, index=parent_codes.index(text(current.up_sbj_area_cd)))
        display_order = st.number_input("표시 순서", min_value=0, step=1, value=0 if pd.isna(current.disp_ord) else int(current.disp_ord))
        predecessors = st.multiselect("선행 주제영역", [item for item in execution_codes if item != text(current.sbj_area_cd)], default=[item for item in selected_predecessors if item in execution_codes], disabled=is_parent)
        active = st.toggle("사용", value=bool(current.active_yn))
        saved = st.form_submit_button("저장", type="primary")
    if saved:
        try:
            save_subject_area(target, metadata_schema, {"old_code": text(current.sbj_area_cd), "code": text(code), "name": text(name), "parent": text(parent), "display_order": int(display_order), "predecessors": predecessors, "active": active, "is_parent": is_parent})
            if text(code).upper() != text(current.sbj_area_cd).upper():
                st.session_state.subject_area_code_renamed = True
            st.rerun()
        except Exception as error:
            st.error(str(error), icon=":material/error:")

elif view == "⚙️ DAG 생성":
    st.subheader("⚙️ DAG 생성")
    dag_mode = st.segmented_control("DAG 생성 업무", ["주제영역 DAG", "프로젝트 오케스트레이터"], default="주제영역 DAG", label_visibility="collapsed")
    if dag_mode == "주제영역 DAG":
        render_dag_generator(subject_areas(target, metadata_schema), target, metadata_schema, connect, qualified, True)
    else:
        render_project_dag_generator(subject_areas(target, metadata_schema), maps, True)

elif view == "✅ 검증":
    st.subheader("✅ 검증")
    render_validation(target, metadata_schema, query_frame, qualified)

elif view == "📦 산출물":
    st.subheader("📦 산출물")
    render_artifacts(target, metadata_schema, True, query_frame, connect, qualified)

elif view == "🔗 테이블·컬럼 매핑":
    st.subheader("🔗 테이블·컬럼 매핑")
    render_mapping_workspace(maps, target, metadata_schema, lambda project, subject_area: True, query_frame, connect, qualified)

elif view == "🔌 접속정보":
    st.subheader("🔌 접속정보")
    render_connection_management(target, metadata_schema, query_frame, connect, qualified)

elif view == "📋 실행 이력":
    st.subheader("📋 실행 이력")
    st.dataframe(run_logs(target, metadata_schema).rename(columns={"wrk_dt": "작업일자", "dag_nm": "DAG", "dag_run_id": "DAG 실행", "task_nm": "태스크", "mpg_id": "매핑 ID", "manf_id": "매니페스트 ID", "meta_ver_no": "버전", "s3_manf_path": "S3 매니페스트", "load_mthd_cd": "실행 방식", "ins_scope_cd": "대상적재범위", "sql_file_path": "SQL 경로", "log_file_path": "로그 경로", "wrk_cnd_val": "작업 조건", "wrk_step_cd": "작업 단계", "wrk_sts_cd": "작업 상태", "src_row_cnt": "원천 건수", "tgt_row_cnt": "대상 건수", "src_size_byte": "원천 크기", "tgt_size_byte": "대상 크기", "wrk_stt_dtm": "시작일시", "wrk_end_dtm": "종료일시", "wrk_elps_sec": "경과초", "wrk_msg": "메시지"}), hide_index=True, height=560)

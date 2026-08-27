from __future__ import annotations

import re
from typing import Any

import pandas as pd
import streamlit as st

from SrcTgtArtifact import render_artifacts
from SrcTgtConnection import render_connection_management
from SrcTgtDagGenerator import render_dag_generator
from SrcTgtMapping import render_mapping_workspace
from SrcTgtRuntime import connect, qualified, query_frame, runtime_context, text
from SrcTgtValidation import render_validation


TABLE_COLUMNS = [
    "mpg_id", "prj_cd", "sbj_area_cd", "tgt_conn_id", "tgt_sch_nm", "tgt_tbl_nm", "tgt_tbl_cmt", "src_conn_id", "src_sch_nm", "src_tbl_nm",
    "load_sts_cd", "sys_col_nm_arr", "sys_col_fmt_cd", "incr_mthd_cd", "src_incr_col_nm_arr", "parl_mthd_cd", "parl_cnd_arr", "meta_ver_no",
]


def area_code(value: object) -> str:
    code = text(value).upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,19}", code):
        raise ValueError("주제영역코드는 영문으로 시작하는 영문·숫자·밑줄 1~20자리여야 합니다.")
    return code


def table_maps(values: dict[str, Any], schema_name: str) -> pd.DataFrame:
    selected = ", ".join(TABLE_COLUMNS)
    return query_frame(values, f"SELECT {selected} FROM {qualified(schema_name, 'tb_mig_tbl_mpg')} WHERE active_yn = TRUE ORDER BY prj_cd, sbj_area_cd, mpg_id")


def subject_areas(values: dict[str, Any], schema_name: str) -> pd.DataFrame:
    return query_frame(values, f"SELECT sbj_area_cd, sbj_area_nm, up_sbj_area_cd, disp_ord, active_yn, crt_dtm, upd_dtm FROM {qualified(schema_name, 'tb_mig_sbj_area')} ORDER BY disp_ord, sbj_area_cd")


def save_subject_area(values: dict[str, Any], schema_name: str, code: object, name: object, parent: object, display_order: int, active: bool) -> None:
    subject = area_code(code)
    label = text(name)
    if not label:
        raise ValueError("주제영역명은 필수입니다.")
    parent_code = text(parent).upper() or None
    if parent_code == subject:
        raise ValueError("상위 주제영역은 자기 자신일 수 없습니다.")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM {qualified(schema_name, 'tb_mig_sbj_area')} WHERE sbj_area_cd = %s", (subject,))
            if cursor.fetchone() is None:
                cursor.execute(f"INSERT INTO {qualified(schema_name, 'tb_mig_sbj_area')} (sbj_area_cd, sbj_area_nm, up_sbj_area_cd, disp_ord, active_yn) VALUES (%s, %s, %s, %s, %s)", (subject, label, parent_code, int(display_order), active))
            else:
                cursor.execute(f"UPDATE {qualified(schema_name, 'tb_mig_sbj_area')} SET sbj_area_nm = %s, up_sbj_area_cd = %s, disp_ord = %s, active_yn = %s, upd_dtm = GETDATE() WHERE sbj_area_cd = %s", (label, parent_code, int(display_order), active, subject))
        connection.commit()


def render_subject_area(values: dict[str, Any], schema_name: str) -> None:
    areas = subject_areas(values, schema_name)
    st.dataframe(areas.rename(columns={"sbj_area_cd": "주제영역코드", "sbj_area_nm": "주제영역명", "up_sbj_area_cd": "상위주제영역코드", "disp_ord": "표시순서", "active_yn": "사용", "crt_dtm": "등록일시", "upd_dtm": "수정일시"}), hide_index=True, height=280)
    options = ["신규", *areas.sbj_area_cd.tolist()]
    selected = st.selectbox("주제영역", options, format_func=lambda value: "신규 주제영역" if value == "신규" else f"{value} · {text(areas.loc[areas.sbj_area_cd.eq(value)].iloc[0].sbj_area_nm)}")
    current = None if selected == "신규" else areas.loc[areas.sbj_area_cd.eq(selected)].iloc[0]
    parent_options = ["", *areas.loc[areas.sbj_area_cd.ne(selected), "sbj_area_cd"].tolist()]
    with st.form("subject_area_form"):
        code = st.text_input("주제영역코드", value="" if current is None else text(current.sbj_area_cd), disabled=current is not None)
        name = st.text_input("주제영역명", value="" if current is None else text(current.sbj_area_nm))
        parent_default = "" if current is None else text(current.up_sbj_area_cd)
        parent = st.selectbox("상위주제영역", parent_options, index=parent_options.index(parent_default) if parent_default in parent_options else 0)
        display_order = st.number_input("표시순서", min_value=0, value=0 if current is None else int(current.disp_ord or 0))
        active = st.toggle("사용", value=True if current is None else bool(current.active_yn))
        saved = st.form_submit_button("저장", type="primary", icon=":material/save:")
    if saved:
        try:
            save_subject_area(values, schema_name, code if current is None else current.sbj_area_cd, name, parent, int(display_order), active)
            st.success("주제영역을 저장했습니다.", icon=":material/check_circle:")
            st.rerun()
        except Exception as error:
            st.error(f"주제영역 저장 실패: {error}", icon=":material/error:")


def run_logs(values: dict[str, Any], schema_name: str) -> pd.DataFrame:
    return query_frame(values, f'''SELECT dag_nm, dag_run_id, mpg_id, task_nm, wrk_dvsn_cd, load_mthd_cd, wrk_sts_cd, src_row_cnt, tgt_row_cnt, s3_byte_size, s3_mnf_path, sql_file_path, src_where_cnd, wrk_stt_dtm, wrk_end_dtm, wrk_elps_sec, wrk_msg
                                     FROM {qualified(schema_name, 'tb_mig_run_log')}
                                    ORDER BY run_hist_id DESC
                                    LIMIT 500''')


st.title("🧭 이관 관리")
st.caption("⚙️ Created by ♡홍율파파♡")
views = ["🔌 접속정보", "🗂️ 주제영역", "🔗 SRC·TGT 매핑", "⚙️ DAG 생성", "✅ 검증", "📋 실행 이력", "📦 산출물"]
view = st.segmented_control("업무", views, default=views[0], label_visibility="collapsed")

try:
    context = runtime_context()
except Exception:
    st.info("초기 설정 메뉴에서 메타 연결과 스키마를 준비한 뒤 다시 선택하십시오.", icon=":material/settings:")
    st.stop()

try:
    maps = table_maps(context.values, context.schema_name)
except Exception as error:
    st.error(f"메타데이터 조회 실패: {error}", icon=":material/error:")
    st.stop()

if view == "🔌 접속정보":
    render_connection_management(context.values, context.schema_name, query_frame, connect, qualified)
elif view == "🗂️ 주제영역":
    render_subject_area(context.values, context.schema_name)
elif view == "🔗 SRC·TGT 매핑":
    render_mapping_workspace(maps, context.values, context.schema_name, query_frame, connect, qualified)
elif view == "⚙️ DAG 생성":
    render_dag_generator(subject_areas(context.values, context.schema_name), maps, context.values, context.schema_name, query_frame, connect, qualified)
elif view == "✅ 검증":
    render_validation(context.values, context.schema_name, query_frame, qualified)
elif view == "📋 실행 이력":
    try:
        st.dataframe(run_logs(context.values, context.schema_name).rename(columns={"dag_nm": "DAG명", "dag_run_id": "DAG실행ID", "mpg_id": "테이블매핑ID", "task_nm": "태스크명", "wrk_dvsn_cd": "작업구분", "load_mthd_cd": "적재방식", "wrk_sts_cd": "작업상태", "src_row_cnt": "원천건수", "tgt_row_cnt": "대상건수", "s3_byte_size": "S3크기", "s3_mnf_path": "S3매니페스트", "sql_file_path": "SQL경로", "src_where_cnd": "원천조회조건", "wrk_stt_dtm": "작업시작일시", "wrk_end_dtm": "작업종료일시", "wrk_elps_sec": "실행경과초", "wrk_msg": "작업메시지"}), hide_index=True, height=600)
    except Exception as error:
        st.error(f"실행 이력 조회 실패: {error}", icon=":material/error:")
elif view == "📦 산출물":
    render_artifacts(context.values, context.schema_name, query_frame, connect, qualified)

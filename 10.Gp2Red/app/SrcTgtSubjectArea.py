from __future__ import annotations

import re
from typing import Any, Callable

import pandas as pd
import streamlit as st

from SrcTgtConnection import connection_frame, connection_label, validate_mapping_connections
from SrcTgtRuntime import connect, qualified, query_frame, text


def area_code(value: object) -> str:
    code = text(value).upper()
    if not re.fullmatch(r"[A-Z][0-9]{2}(?:[0-9]{4})?", code):
        raise ValueError("상위주제영역코드는 A01 형식, 주제영역코드는 A010001 형식이어야 합니다.")
    return code


def area_kind(code: object) -> str:
    return "PARENT" if len(area_code(code)) == 3 else "CHILD"


def subject_areas(values: dict[str, Any], schema_name: str) -> pd.DataFrame:
    return query_frame(values, f"SELECT A.sbj_area_cd, A.sbj_area_nm, A.up_sbj_area_cd, P.sbj_area_nm AS up_sbj_area_nm, A.src_conn_id, A.tgt_conn_id, A.disp_ord, A.active_yn, A.crt_dtm, A.upd_dtm FROM {qualified(schema_name, 'tb_mig_sbj_area')} A LEFT JOIN {qualified(schema_name, 'tb_mig_sbj_area')} P ON P.sbj_area_cd = A.up_sbj_area_cd ORDER BY A.disp_ord, A.sbj_area_cd")


def save_subject_area(values: dict[str, Any], schema_name: str, code: object, name: object, parent: object, source_connection_id: object, target_connection_id: object, display_order: int, active: bool) -> None:
    subject = area_code(code)
    label = text(name)
    if not label:
        raise ValueError("주제영역명은 필수입니다.")
    kind = area_kind(subject)
    parent_code = text(parent).upper() or None
    if kind == "PARENT" and parent_code:
        raise ValueError("상위주제영역에는 상위주제영역을 지정할 수 없습니다.")
    if kind == "CHILD" and not parent_code:
        raise ValueError("주제영역에는 상위주제영역이 필수입니다.")
    if parent_code == subject:
        raise ValueError("상위주제영역은 자기 자신일 수 없습니다.")
    if kind == "CHILD" and parent_code and not subject.startswith(parent_code):
        raise ValueError("주제영역코드는 선택한 상위주제영역코드로 시작해야 합니다.")
    source_connection = text(source_connection_id).upper() or None
    target_connection = text(target_connection_id).upper() or None
    if kind == "PARENT" and (source_connection or target_connection):
        raise ValueError("접속정보는 주제영역에만 지정하십시오.")
    if bool(source_connection) != bool(target_connection):
        raise ValueError("원천접속ID와 대상접속ID는 함께 입력하십시오.")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            if parent_code:
                cursor.execute(f"SELECT 1 FROM {qualified(schema_name, 'tb_mig_sbj_area')} WHERE sbj_area_cd = %s AND active_yn = TRUE", (parent_code,))
                if cursor.fetchone() is None:
                    raise ValueError(f"사용 중인 상위주제영역을 찾을 수 없습니다: {parent_code}")
            if source_connection and target_connection:
                validate_mapping_connections(cursor, schema_name, qualified, source_connection, target_connection)
            cursor.execute(f"SELECT 1 FROM {qualified(schema_name, 'tb_mig_sbj_area')} WHERE sbj_area_cd = %s", (subject,))
            if cursor.fetchone() is None:
                cursor.execute(f"INSERT INTO {qualified(schema_name, 'tb_mig_sbj_area')} (sbj_area_cd, sbj_area_nm, up_sbj_area_cd, src_conn_id, tgt_conn_id, disp_ord, active_yn) VALUES (%s, %s, %s, %s, %s, %s, %s)", (subject, label, parent_code, source_connection, target_connection, int(display_order), active))
            else:
                cursor.execute(f"UPDATE {qualified(schema_name, 'tb_mig_sbj_area')} SET sbj_area_nm = %s, up_sbj_area_cd = %s, src_conn_id = %s, tgt_conn_id = %s, disp_ord = %s, active_yn = %s, upd_dtm = GETDATE() WHERE sbj_area_cd = %s", (label, parent_code, source_connection, target_connection, int(display_order), active, subject))
        connection.commit()


def render_subject_area(values: dict[str, Any], schema_name: str) -> None:
    areas = subject_areas(values, schema_name)
    connections = connection_frame(query_frame, values, schema_name, qualified, active_only=True)
    st.dataframe(areas.rename(columns={"sbj_area_cd": "주제영역코드", "sbj_area_nm": "주제영역명", "up_sbj_area_cd": "상위주제영역코드", "up_sbj_area_nm": "상위주제영역명", "src_conn_id": "원천접속ID", "tgt_conn_id": "대상접속ID", "disp_ord": "표시순서", "active_yn": "사용", "crt_dtm": "등록일시", "upd_dtm": "수정일시"}), hide_index=True, height=280)
    options = ["신규", *areas.sbj_area_cd.tolist()]
    selected = st.selectbox("주제영역", options, format_func=lambda value: "신규 주제영역" if value == "신규" else f"{value} · {text(areas.loc[areas.sbj_area_cd.eq(value)].iloc[0].sbj_area_nm)}")
    current = None if selected == "신규" else areas.loc[areas.sbj_area_cd.eq(selected)].iloc[0]
    current_kind = "CHILD" if current is None else area_kind(current.sbj_area_cd)
    parent_options = ["", *areas.loc[areas.sbj_area_cd.map(lambda value: area_kind(value) == "PARENT") & areas.sbj_area_cd.ne(selected), "sbj_area_cd"].tolist()]
    with st.form("subject_area_form"):
        kind_options = ["상위주제영역", "주제영역"]
        kind_label = st.segmented_control("영역 구분", kind_options, default="상위주제영역" if current_kind == "PARENT" else "주제영역", disabled=current is not None)
        kind = "PARENT" if kind_label == "상위주제영역" else "CHILD"
        code = st.text_input("상위주제영역코드" if kind == "PARENT" else "주제영역코드", value="" if current is None else text(current.sbj_area_cd), disabled=current is not None)
        name = st.text_input("상위주제영역명" if kind == "PARENT" else "주제영역명", value="" if current is None else text(current.sbj_area_nm))
        parent_default = "" if current is None else text(current.up_sbj_area_cd)
        parent = st.selectbox("상위주제영역", parent_options, index=parent_options.index(parent_default) if parent_default in parent_options else 0, format_func=lambda value: "선택" if not value else f"{value} · {text(areas.loc[areas.sbj_area_cd.eq(value)].iloc[0].sbj_area_nm)}", disabled=kind == "PARENT")
        connection_options = ["", *connections.conn_id.tolist()]
        source_default = "" if current is None else text(current.src_conn_id)
        target_default = "" if current is None else text(current.tgt_conn_id)
        source_connection = st.selectbox("원천접속ID", connection_options, index=connection_options.index(source_default) if source_default in connection_options else 0, format_func=lambda value: "선택 안 함" if not value else connection_label(connections, value), disabled=kind == "PARENT")
        target_connection = st.selectbox("대상접속ID", connection_options, index=connection_options.index(target_default) if target_default in connection_options else 0, format_func=lambda value: "선택 안 함" if not value else connection_label(connections, value), disabled=kind == "PARENT")
        display_order = st.number_input("표시순서", min_value=0, value=0 if current is None else int(current.disp_ord or 0))
        active = st.toggle("사용", value=True if current is None else bool(current.active_yn))
        saved = st.form_submit_button("저장", type="primary", icon=":material/save:")
    if saved:
        try:
            save_subject_area(values, schema_name, code if current is None else current.sbj_area_cd, name, None if kind == "PARENT" else parent, None if kind == "PARENT" else source_connection, None if kind == "PARENT" else target_connection, int(display_order), active)
            st.success("주제영역을 저장했습니다.", icon=":material/check_circle:")
            st.rerun()
        except Exception as error:
            st.error(f"주제영역 저장 실패: {error}", icon=":material/error:")

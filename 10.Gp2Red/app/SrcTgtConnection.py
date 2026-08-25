from __future__ import annotations

import re
from typing import Any, Callable

import pandas as pd
import streamlit as st

from SrcTgtSecurity import text


CONNECTION_TYPES = {"원천": "SRC", "대상": "TGT"}
DBMS_CODES = ["GREENPLUM", "REDSHIFT", "ORACLE", "MSSQL", "POSTGRESQL", "OTHER"]


def connection_id(value: object) -> str:
    candidate = text(value).upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,99}", candidate):
        raise ValueError("접속ID는 영문으로 시작하는 영문·숫자·밑줄 1~100자리여야 합니다.")
    return candidate


def reference_name(value: object, label: str) -> str:
    candidate = text(value)
    if not candidate or "\x00" in candidate or len(candidate) > 100:
        raise ValueError(f"{label}은(는) 1~100자로 입력하십시오.")
    return candidate


def connection_frame(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], active_only: bool = False) -> pd.DataFrame:
    condition = "WHERE active_yn = TRUE" if active_only else ""
    query = f'''SELECT conn_id, conn_nm, conn_dvsn_cd, dbms_cd, sec_sect_nm, af_conn_id, active_yn, crt_dtm, upd_dtm
                  FROM {qualified(schema_name, "tb_mig_conn")}
                  {condition}
                 ORDER BY conn_dvsn_cd, conn_id'''
    return query_frame(values, query)


def selectable_connections(frame: pd.DataFrame, division: str, current_id: object = None) -> pd.DataFrame:
    normalized = text(division).upper()
    result = frame.loc[frame.conn_dvsn_cd.map(text).str.upper().eq(normalized) & frame.active_yn.fillna(False).astype(bool)].copy()
    current = text(current_id).upper()
    if current and current not in result.conn_id.map(text).str.upper().tolist():
        missing = pd.DataFrame([{"conn_id": current, "conn_nm": "미등록 또는 미사용 접속", "conn_dvsn_cd": normalized, "dbms_cd": "", "sec_sect_nm": "", "af_conn_id": "", "active_yn": False}])
        result = pd.concat([missing, result], ignore_index=True)
    return result.sort_values("conn_id").reset_index(drop=True)


def connection_label(frame: pd.DataFrame, selected_id: str) -> str:
    row = frame.loc[frame.conn_id.map(text).str.upper().eq(text(selected_id).upper())].iloc[0]
    return f"{text(row.conn_id)} · {text(row.conn_nm)} · {text(row.dbms_cd)}"


def connection_ids(frame: pd.DataFrame, division: str) -> list[str]:
    return [text(value) for value in selectable_connections(frame, division).conn_id.tolist()]


def runtime_connection_values(frame: pd.DataFrame, selected_id: str) -> dict[str, Any]:
    row = frame.loc[frame.conn_id.map(text).str.upper().eq(text(selected_id).upper())]
    if row.empty or not bool(row.iloc[0].active_yn):
        raise ValueError("사용 중인 접속정보를 선택하십시오.")
    section = reference_name(row.iloc[0].sec_sect_nm, "Secrets 섹션명")
    if section not in st.secrets:
        raise ValueError(f".streamlit/secrets.toml에 [{section}] 설정이 없습니다.")
    values = dict(st.secrets[section])
    required = ("host", "port", "database", "user", "password")
    if [key for key in required if not text(values.get(key))]:
        raise ValueError(f"[{section}] 필수 접속 항목이 없습니다.")
    return values


def validate_mapping_connections(cursor: Any, schema_name: str, qualified: Callable[[str, str], str], source_id: object, target_id: object) -> None:
    table_name = qualified(schema_name, "tb_mig_conn")
    for division, value, label in (("SRC", source_id, "원천접속ID"), ("TGT", target_id, "대상접속ID")):
        candidate = connection_id(value)
        cursor.execute(f"SELECT 1 FROM {table_name} WHERE conn_id = %s AND conn_dvsn_cd = %s AND active_yn = TRUE", (candidate, division))
        if cursor.fetchone() is None:
            raise ValueError(f"사용 중인 {label}를 접속관리에서 선택하거나 등록하십시오: {candidate}")


def save_connection(connect: Callable[[dict[str, Any]], Any], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], record: dict[str, object], user_id: str) -> None:
    item_id = connection_id(record["conn_id"])
    name = text(record["conn_nm"])
    if not name or len(name) > 200:
        raise ValueError("접속명은 1~200자로 입력하십시오.")
    division = text(record["conn_dvsn_cd"]).upper()
    if division not in set(CONNECTION_TYPES.values()):
        raise ValueError("접속구분을 선택하십시오.")
    dbms = text(record["dbms_cd"]).upper()
    if dbms not in DBMS_CODES:
        raise ValueError("DBMS를 선택하십시오.")
    section = reference_name(record["sec_sect_nm"], "Secrets 섹션명")
    airflow = reference_name(record["af_conn_id"], "Airflow 접속ID")
    table_name = qualified(schema_name, "tb_mig_conn")
    mapping_table = qualified(schema_name, "tb_mig_tbl_mpg")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT conn_id FROM {table_name} WHERE conn_id = %s", (item_id,))
            existing = cursor.fetchone()
            if not bool(record["active_yn"]):
                column_name = "src_conn_id" if division == "SRC" else "tgt_conn_id"
                cursor.execute(f"SELECT 1 FROM {mapping_table} WHERE {column_name} = %s AND active_yn = TRUE LIMIT 1", (item_id,))
                if cursor.fetchone() is not None:
                    raise ValueError("사용 중인 테이블매핑이 있어 접속정보를 사용 중지할 수 없습니다.")
            if existing is None:
                cursor.execute(f"INSERT INTO {table_name} (conn_id, conn_nm, conn_dvsn_cd, dbms_cd, sec_sect_nm, af_conn_id, active_yn, crt_by, upd_by) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (item_id, name, division, dbms, section, airflow, bool(record["active_yn"]), user_id, user_id))
            else:
                cursor.execute(f"UPDATE {table_name} SET conn_nm = %s, conn_dvsn_cd = %s, dbms_cd = %s, sec_sect_nm = %s, af_conn_id = %s, active_yn = %s, upd_by = %s, upd_dtm = GETDATE() WHERE conn_id = %s", (name, division, dbms, section, airflow, bool(record["active_yn"]), user_id, item_id))
        connection.commit()


def render_connection_management(values: dict[str, Any], schema_name: str, user_id: str, query_frame: Callable[..., pd.DataFrame], connect: Callable[[dict[str, Any]], Any], qualified: Callable[[str, str], str]) -> None:
    connections = connection_frame(query_frame, values, schema_name, qualified)
    displayed = connections.rename(columns={"conn_id": "접속 ID", "conn_nm": "접속명", "conn_dvsn_cd": "접속구분", "dbms_cd": "DBMS", "sec_sect_nm": "Secrets 섹션명", "af_conn_id": "Airflow 접속 ID", "active_yn": "사용", "crt_dtm": "등록일시", "upd_dtm": "수정일시"})
    st.dataframe(displayed, hide_index=True, height=260)
    mode = st.segmented_control("접속 관리 업무", ["접속 등록", "접속 수정"], default="접속 등록", label_visibility="collapsed")
    current = None
    if mode == "접속 수정":
        if connections.empty:
            st.info("등록된 접속정보가 없습니다.", icon=":material/info:")
            return
        selected = st.selectbox("접속", connections.conn_id.tolist(), format_func=lambda value: connection_label(connections, value))
        current = connections.loc[connections.conn_id.eq(selected)].iloc[0]
    with st.form("mig_connection_form"):
        if current is None:
            item_id = st.text_input("접속 ID", placeholder="예: SRC_ORA_01")
        else:
            item_id = st.text_input("접속 ID", value=text(current.conn_id), disabled=True)
        name = st.text_input("접속명", value="" if current is None else text(current.conn_nm))
        division_keys = list(CONNECTION_TYPES)
        current_division = "원천" if current is None or text(current.conn_dvsn_cd).upper() == "SRC" else "대상"
        division_name = st.selectbox("접속구분", division_keys, index=division_keys.index(current_division))
        dbms = st.selectbox("DBMS", DBMS_CODES, index=0 if current is None or text(current.dbms_cd).upper() not in DBMS_CODES else DBMS_CODES.index(text(current.dbms_cd).upper()))
        section = st.text_input("Secrets 섹션명", value="" if current is None else text(current.sec_sect_nm))
        airflow = st.text_input("Airflow 접속 ID", value="" if current is None else text(current.af_conn_id))
        active = st.toggle("사용", value=True if current is None else bool(current.active_yn))
        saved = st.form_submit_button("접속정보 저장", type="primary", icon=":material/save:")
    if saved:
        try:
            save_connection(connect, values, schema_name, qualified, {"conn_id": item_id, "conn_nm": name, "conn_dvsn_cd": CONNECTION_TYPES[division_name], "dbms_cd": dbms, "sec_sect_nm": section, "af_conn_id": airflow, "active_yn": active}, user_id)
            st.success("접속정보를 저장했습니다.", icon=":material/check_circle:")
            st.rerun()
        except Exception as error:
            st.error(f"접속정보 저장 실패: {error}", icon=":material/error:")

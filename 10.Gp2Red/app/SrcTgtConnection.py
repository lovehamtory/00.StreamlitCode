from __future__ import annotations

import re
from typing import Any, Callable

import pandas as pd
import streamlit as st

from SrcTgtRuntime import text


DBMS_CODES = ["GREENPLUM", "REDSHIFT", "ORACLE", "MSSQL", "POSTGRESQL", "OTHER"]


def connection_id(value: object) -> str:
    candidate = text(value).upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,99}", candidate):
        raise ValueError("접속 ID는 영문으로 시작하는 영문·숫자·밑줄 1~100자리여야 합니다.")
    return candidate


def reference_name(value: object, label: str) -> str:
    candidate = text(value)
    if not candidate or "\x00" in candidate or len(candidate) > 100:
        raise ValueError(f"{label}은(는) 1~100자로 입력하십시오.")
    return candidate


def character_length_multiple(value: object) -> int:
    try:
        multiple = int(value or 3)
    except (TypeError, ValueError) as error:
        raise ValueError("문자길이배수는 1~4 정수여야 합니다.") from error
    if multiple not in {1, 2, 3, 4}:
        raise ValueError("문자길이배수는 1, 2, 3, 4 중 하나여야 합니다.")
    return multiple


def connection_frame(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], active_only: bool = False) -> pd.DataFrame:
    condition = "WHERE active_yn = TRUE" if active_only else ""
    query = f'''SELECT conn_id, conn_nm, dbms_cd, char_len_mul, s3_stg_path, sec_sect_nm, active_yn, crt_dtm, upd_dtm
                  FROM {qualified(schema_name, "tb_mig_conn")}
                  {condition}
                 ORDER BY conn_id'''
    return query_frame(values, query)


def selectable_connections(frame: pd.DataFrame, current_id: object = None) -> pd.DataFrame:
    result = frame.loc[frame.active_yn.fillna(False).astype(bool)].copy()
    current = text(current_id).upper()
    if current and current not in result.conn_id.map(text).str.upper().tolist():
        missing = pd.DataFrame([{"conn_id": current, "conn_nm": "미등록 또는 미사용 접속", "dbms_cd": "", "char_len_mul": 3, "s3_stg_path": "", "sec_sect_nm": "", "active_yn": False}])
        result = pd.concat([missing, result], ignore_index=True)
    return result.sort_values("conn_id").reset_index(drop=True)


def connection_label(frame: pd.DataFrame, selected_id: str) -> str:
    row = frame.loc[frame.conn_id.map(text).str.upper().eq(text(selected_id).upper())].iloc[0]
    return f"{text(row.conn_id)} · {text(row.conn_nm)} · {text(row.dbms_cd)}"


def connection_ids(frame: pd.DataFrame) -> list[str]:
    return [text(value) for value in selectable_connections(frame).conn_id.tolist()]


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
    for value, label in ((source_id, "원천접속 ID"), (target_id, "대상접속 ID")):
        candidate = connection_id(value)
        cursor.execute(f"SELECT 1 FROM {table_name} WHERE conn_id = %s AND active_yn = TRUE", (candidate,))
        if cursor.fetchone() is None:
            raise ValueError(f"사용 중인 {label}를 접속정보에서 선택하거나 등록하십시오: {candidate}")
    cursor.execute(f"SELECT s3_stg_path FROM {table_name} WHERE conn_id = %s", (connection_id(target_id),))
    path_row = cursor.fetchone()
    if path_row is None or not text(path_row[0]).startswith("s3://"):
        raise ValueError("대상접속에는 S3 기준경로를 입력해야 합니다.")


def save_connection(connect: Callable[[dict[str, Any]], Any], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], record: dict[str, object]) -> None:
    item_id = connection_id(record["conn_id"])
    name = text(record["conn_nm"])
    if not name or len(name) > 200:
        raise ValueError("접속명은 1~200자로 입력하십시오.")
    dbms = text(record["dbms_cd"]).upper()
    if dbms not in DBMS_CODES:
        raise ValueError("DBMS를 선택하십시오.")
    char_multiple = character_length_multiple(record.get("char_len_mul", 3))
    base_path = text(record["s3_stg_path"]).rstrip("/") or None
    if base_path and not base_path.startswith("s3://"):
        raise ValueError("S3 기준경로는 s3://로 시작해야 합니다.")
    section = reference_name(record["sec_sect_nm"], "Secrets 섹션명")
    table_name = qualified(schema_name, "tb_mig_conn")
    subject_table = qualified(schema_name, "tb_mig_sbj_area")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT conn_id FROM {table_name} WHERE conn_id = %s", (item_id,))
            existing = cursor.fetchone()
            if not bool(record["active_yn"]):
                cursor.execute(f"SELECT 1 FROM {subject_table} WHERE (src_conn_id = %s OR tgt_conn_id = %s) AND active_yn = TRUE LIMIT 1", (item_id, item_id))
                if cursor.fetchone() is not None:
                    raise ValueError("사용 중인 주제영역이 있어 접속정보를 사용 중지할 수 없습니다.")
            arguments = (name, dbms, char_multiple, base_path, section, bool(record["active_yn"]), item_id)
            if existing is None:
                cursor.execute(f"INSERT INTO {table_name} (conn_id, conn_nm, dbms_cd, char_len_mul, s3_stg_path, sec_sect_nm, active_yn) VALUES (%s, %s, %s, %s, %s, %s, %s)", (item_id, *arguments[:-1]))
            else:
                cursor.execute(f"UPDATE {table_name} SET conn_nm = %s, dbms_cd = %s, char_len_mul = %s, s3_stg_path = %s, sec_sect_nm = %s, active_yn = %s, upd_dtm = GETDATE() WHERE conn_id = %s", arguments)
        connection.commit()


def render_connection_management(values: dict[str, Any], schema_name: str, query_frame: Callable[..., pd.DataFrame], connect: Callable[[dict[str, Any]], Any], qualified: Callable[[str, str], str]) -> None:
    connections = connection_frame(query_frame, values, schema_name, qualified)
    displayed = connections.rename(columns={"conn_id": "접속 ID", "conn_nm": "접속명", "dbms_cd": "DBMS", "char_len_mul": "문자길이배수", "s3_stg_path": "S3 기준경로", "sec_sect_nm": "Secrets 섹션명", "active_yn": "사용", "crt_dtm": "등록일시", "upd_dtm": "수정일시"})
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
        item_id = st.text_input("접속 ID", value="" if current is None else text(current.conn_id), disabled=current is not None, placeholder="예: GP_SRC_01 또는 RED_TGT_01")
        name = st.text_input("접속명", value="" if current is None else text(current.conn_nm))
        dbms_value = "GREENPLUM" if current is None else text(current.dbms_cd).upper()
        dbms = st.selectbox("DBMS", DBMS_CODES, index=DBMS_CODES.index(dbms_value) if dbms_value in DBMS_CODES else 0)
        char_len_mul = st.number_input("문자길이배수", min_value=1, max_value=4, step=1, value=3 if current is None else character_length_multiple(current.get("char_len_mul", 3)), help="원천 문자형 길이에만 적용합니다. 한글 UTF-8 기준은 보통 3입니다.")
        s3_stg_path = st.text_input("S3 기준경로", value="" if current is None else text(current.s3_stg_path), placeholder="s3://bucket/prefix", help="대상으로 선택할 접속에만 입력합니다.")
        section = st.text_input("Secrets 섹션명", value="" if current is None else text(current.sec_sect_nm))
        active = st.toggle("사용", value=True if current is None else bool(current.active_yn))
        saved = st.form_submit_button("접속정보 저장", type="primary", icon=":material/save:")
    if saved:
        try:
            save_connection(connect, values, schema_name, qualified, {"conn_id": item_id, "conn_nm": name, "dbms_cd": dbms, "char_len_mul": char_len_mul, "s3_stg_path": s3_stg_path, "sec_sect_nm": section, "active_yn": active})
            st.success("접속정보를 저장했습니다.", icon=":material/check_circle:")
            st.rerun()
        except Exception as error:
            st.error(f"접속정보 저장 실패: {error}", icon=":material/error:")

from __future__ import annotations

import re
from typing import Any, Callable

import pandas as pd
import streamlit as st

from SrcTgtRuntime import connect, qualified, text


EMR_TYPES = ["EMR_EC2", "EMR_SERVERLESS"]


def emr_id(value: object) -> str:
    candidate = text(value).upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,99}", candidate):
        raise ValueError("EMR ID는 영문으로 시작하는 영문·숫자·밑줄 1~100자리여야 합니다.")
    return candidate


def emr_frame(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified_name: Callable[[str, str], str], active_only: bool = False) -> pd.DataFrame:
    condition = "WHERE active_yn = TRUE" if active_only else ""
    return query_frame(values, f"SELECT emr_id, emr_nm, emr_type_cd, aws_conn_id, sec_sect_nm, emr_cluster_id, dedicated_yn, auto_term_yn, idle_term_min, active_yn, crt_dtm, upd_dtm FROM {qualified_name(schema_name, 'tb_mig_emr')} {condition} ORDER BY emr_id")


def emr_secret_settings(profile: dict[str, Any]) -> dict[str, Any]:
    section = text(profile.get("sec_sect_nm"))
    if not section or section not in st.secrets:
        raise ValueError("EMR AWS Secrets 섹션을 확인하십시오.")
    return dict(st.secrets[section])


def terminate_cluster(profile: dict[str, Any], force: bool = True) -> str:
    if not bool(profile.get("dedicated_yn")):
        raise ValueError("공용 EMR은 이 화면에서 강제 종료할 수 없습니다.")
    cluster_id = text(profile.get("emr_cluster_id"))
    if not cluster_id:
        raise ValueError("EMR 클러스터 ID가 없습니다.")
    if text(profile.get("emr_type_cd")).upper() != "EMR_EC2":
        raise ValueError("EMR Serverless 강제 종료는 실행 애플리케이션 단위로 처리하십시오.")
    try:
        import boto3
    except ModuleNotFoundError as error:
        raise RuntimeError("boto3가 설치되지 않았습니다.") from error
    settings = emr_secret_settings(profile)
    options = {key: value for key, value in {"region_name": settings.get("region_name"), "profile_name": settings.get("aws_profile_name")}.items() if text(value)}
    client = boto3.Session(**options).client("emr")
    client.terminate_job_flows(JobFlowIds=[cluster_id])
    return "강제 종료 요청"


def save_emr(values: dict[str, Any], schema_name: str, qualified_name: Callable[[str, str], str], record: dict[str, object]) -> None:
    item_id = emr_id(record["emr_id"])
    name = text(record["emr_nm"])
    emr_type = text(record["emr_type_cd"]).upper()
    aws_connection = text(record["aws_conn_id"])
    section = text(record["sec_sect_nm"])
    cluster = text(record.get("emr_cluster_id")) or None
    idle_minutes = int(record["idle_term_min"])
    if not name or emr_type not in EMR_TYPES or not aws_connection or not section or idle_minutes < 1 or idle_minutes > 1440:
        raise ValueError("EMR 입력값을 확인하십시오.")
    if bool(record["auto_term_yn"]) and not bool(record["dedicated_yn"]):
        raise ValueError("공용 EMR에는 자동 종료를 설정할 수 없습니다.")
    table = qualified_name(schema_name, "tb_mig_emr")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM {table} WHERE emr_id = %s", (item_id,))
            parameters = (name, emr_type, aws_connection, section, cluster, bool(record["dedicated_yn"]), bool(record["auto_term_yn"]), idle_minutes, bool(record["active_yn"]), item_id)
            if cursor.fetchone() is None:
                cursor.execute(f"INSERT INTO {table} (emr_id, emr_nm, emr_type_cd, aws_conn_id, sec_sect_nm, emr_cluster_id, dedicated_yn, auto_term_yn, idle_term_min, active_yn) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (item_id, *parameters[:-1]))
            else:
                cursor.execute(f"UPDATE {table} SET emr_nm = %s, emr_type_cd = %s, aws_conn_id = %s, sec_sect_nm = %s, emr_cluster_id = %s, dedicated_yn = %s, auto_term_yn = %s, idle_term_min = %s, active_yn = %s, upd_dtm = GETDATE() WHERE emr_id = %s", parameters)
        connection.commit()


def render_emr_management(values: dict[str, Any], schema_name: str, query_frame: Callable[..., pd.DataFrame], qualified_name: Callable[[str, str], str]) -> None:
    if not st.button("EMR 정보 조회", icon=":material/search:") and "emr_loaded" not in st.session_state:
        return
    st.session_state.emr_loaded = True
    try:
        frame = emr_frame(query_frame, values, schema_name, qualified_name)
    except Exception as error:
        st.error(f"EMR 정보 조회 실패: {error}", icon=":material/error:")
        return
    st.dataframe(frame.rename(columns={"emr_id": "EMR ID", "emr_nm": "EMR명", "emr_type_cd": "EMR 유형", "aws_conn_id": "Airflow AWS 접속ID", "sec_sect_nm": "AWS Secrets 섹션명", "emr_cluster_id": "클러스터ID", "dedicated_yn": "전용", "auto_term_yn": "자동종료", "idle_term_min": "유휴종료분", "active_yn": "사용"}), hide_index=True)
    selected = st.selectbox("수정 대상", ["신규", *frame.emr_id.tolist()])
    current = None if selected == "신규" else frame.loc[frame.emr_id.eq(selected)].iloc[0]
    with st.form("emr_form"):
        item_id = st.text_input("EMR ID", value="" if current is None else text(current.emr_id), disabled=current is not None)
        name = st.text_input("EMR명", value="" if current is None else text(current.emr_nm))
        type_value = "EMR_EC2" if current is None else text(current.emr_type_cd).upper()
        emr_type = st.selectbox("EMR 유형", EMR_TYPES, index=EMR_TYPES.index(type_value) if type_value in EMR_TYPES else 0)
        aws_conn_id = st.text_input("Airflow AWS 접속ID", value="" if current is None else text(current.aws_conn_id))
        section = st.text_input("AWS Secrets 섹션명", value="" if current is None else text(current.sec_sect_nm))
        cluster_id = st.text_input("EMR 클러스터 ID", value="" if current is None else text(current.emr_cluster_id))
        dedicated = st.toggle("전용 EMR", value=True if current is None else bool(current.dedicated_yn))
        auto_terminate = st.toggle("DAG 종료 후 자동 종료", value=True if current is None else bool(current.auto_term_yn), disabled=not dedicated)
        idle_minutes = st.number_input("유휴 종료 분", min_value=1, max_value=1440, value=30 if current is None else int(current.idle_term_min))
        active = st.toggle("사용", value=True if current is None else bool(current.active_yn))
        saved = st.form_submit_button("EMR 저장", type="primary", icon=":material/save:")
    if saved:
        try:
            save_emr(values, schema_name, qualified_name, {"emr_id": item_id, "emr_nm": name, "emr_type_cd": emr_type, "aws_conn_id": aws_conn_id, "sec_sect_nm": section, "emr_cluster_id": cluster_id, "dedicated_yn": dedicated, "auto_term_yn": auto_terminate, "idle_term_min": idle_minutes, "active_yn": active})
            st.success("EMR 정보를 저장했습니다.", icon=":material/check_circle:")
            st.session_state.emr_loaded = False
            st.rerun()
        except Exception as error:
            st.error(f"EMR 저장 실패: {error}", icon=":material/error:")
    if current is not None and bool(current.dedicated_yn) and text(current.emr_type_cd).upper() == "EMR_EC2":
        confirmed = st.checkbox("전용 EMR 강제 종료 확인", key=f"emr_terminate_{current.emr_id}")
        if st.button("전용 EMR 강제 종료", type="primary", icon=":material/power_settings_new:", disabled=not confirmed):
            try:
                result = terminate_cluster(current.to_dict(), force=True)
                st.success(result, icon=":material/check_circle:")
            except Exception as error:
                st.error(f"EMR 강제 종료 실패: {error}", icon=":material/error:")

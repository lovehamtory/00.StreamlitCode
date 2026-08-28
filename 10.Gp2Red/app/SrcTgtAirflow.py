from __future__ import annotations

import base64
import json
import re
import time
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd
import streamlit as st

from SrcTgtRuntime import connect, qualified, text


DEPLOY_METHODS = ["SHARED_PATH", "DEPLOY_AGENT"]


def airflow_id(value: object) -> str:
    candidate = text(value).upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,99}", candidate):
        raise ValueError("Airflow ID는 영문으로 시작하는 영문·숫자·밑줄 1~100자리여야 합니다.")
    return candidate


def airflow_frame(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified_name: Callable[[str, str], str], active_only: bool = False) -> pd.DataFrame:
    condition = "WHERE active_yn = TRUE" if active_only else ""
    return query_frame(values, f"SELECT airflow_id, airflow_nm, dply_mthd_cd, sec_sect_nm, active_yn, crt_dtm, upd_dtm FROM {qualified_name(schema_name, 'tb_mig_airflow')} {condition} ORDER BY airflow_id")


def secret_settings(profile: dict[str, Any]) -> dict[str, Any]:
    section = text(profile.get("sec_sect_nm"))
    if not section or section not in st.secrets:
        raise ValueError("Airflow Secrets 섹션을 확인하십시오.")
    return dict(st.secrets[section])


def safe_dag_name(value: object) -> str:
    candidate = text(value)
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,249}", candidate):
        raise ValueError("DAG 파일명을 확인하십시오.")
    return candidate


def deployment_root(settings: dict[str, Any]) -> Path:
    configured = text(settings.get("dag_deploy_root"))
    if not configured:
        raise ValueError("Airflow Secrets에 dag_deploy_root를 설정하십시오.")
    root = Path(configured).expanduser().resolve()
    if str(root) == root.anchor:
        raise ValueError("DAG 배포 경로는 루트 경로일 수 없습니다.")
    return root


def write_shared_sources(settings: dict[str, Any], sources: dict[str, str]) -> list[str]:
    root = deployment_root(settings)
    root.mkdir(parents=True, exist_ok=True)
    paths: list[str] = []
    for name, source in sources.items():
        file_name = safe_dag_name(name) + ".py"
        target = root / file_name
        temporary = target.with_suffix(".tmp")
        temporary.write_text(source, encoding="utf-8")
        temporary.replace(target)
        paths.append(str(target))
    return paths


def deploy_agent_sources(settings: dict[str, Any], sources: dict[str, str]) -> list[str]:
    endpoint = text(settings.get("deploy_agent_url")).rstrip("/")
    token = text(settings.get("deploy_agent_token"))
    if not endpoint or not token:
        raise ValueError("Airflow Secrets에 deploy_agent_url과 deploy_agent_token을 설정하십시오.")
    payload = {"files": [{"name": safe_dag_name(name) + ".py", "content_b64": base64.b64encode(source.encode("utf-8")).decode("ascii")} for name, source in sources.items()]}
    request = Request(endpoint + "/dags", data=json.dumps(payload).encode("utf-8"), method="PUT", headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=30) as response:
            body = json.loads(response.read().decode("utf-8") or "{}")
    except HTTPError as error:
        raise RuntimeError(f"DAG 배포 에이전트 오류: {error.code}") from error
    except URLError as error:
        raise RuntimeError(f"DAG 배포 에이전트 연결 실패: {error.reason}") from error
    paths = body.get("paths")
    if not isinstance(paths, list) or len(paths) != len(sources):
        raise RuntimeError("DAG 배포 에이전트의 파일 결과를 확인할 수 없습니다.")
    return [text(path) for path in paths]


def pause_dags(settings: dict[str, Any], dag_names: list[str]) -> None:
    base_url = text(settings.get("airflow_api_url")).rstrip("/")
    token = text(settings.get("airflow_api_token"))
    username = text(settings.get("airflow_api_username"))
    password = text(settings.get("airflow_api_password"))
    if not base_url:
        raise ValueError("Airflow Secrets에 airflow_api_url을 설정하십시오.")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    elif username or password:
        if not username or not password:
            raise ValueError("Airflow API 사용자ID와 비밀번호를 함께 설정하십시오.")
        encoded = base64.b64encode((username + ":" + password).encode("utf-8")).decode("ascii")
        headers["Authorization"] = "Basic " + encoded
    for dag_name in dag_names:
        for attempt in range(10):
            request = Request(base_url + "/api/v1/dags/" + safe_dag_name(dag_name), data=b'{"is_paused":true}', method="PATCH", headers=headers)
            try:
                with urlopen(request, timeout=30) as response:
                    if response.status in {200, 201}:
                        break
                    raise RuntimeError(f"Airflow DAG 비활성 등록 실패: {dag_name}")
            except HTTPError as error:
                if error.code != 404 or attempt == 9:
                    raise RuntimeError(f"Airflow DAG 비활성 등록 실패: {dag_name} ({error.code})") from error
                time.sleep(3)
            except URLError as error:
                raise RuntimeError(f"Airflow API 연결 실패: {error.reason}") from error


def deploy_sources(profile: dict[str, Any], sources: dict[str, str]) -> list[str]:
    method = text(profile.get("dply_mthd_cd")).upper()
    if method not in DEPLOY_METHODS:
        raise ValueError("DAG 배포방식을 선택하십시오.")
    settings = secret_settings(profile)
    paths = write_shared_sources(settings, sources) if method == "SHARED_PATH" else deploy_agent_sources(settings, sources)
    pause_dags(settings, list(sources))
    return paths


def save_deploy_history(values: dict[str, Any], schema_name: str, qualified_name: Callable[[str, str], str], airflow: object, method: object, sources: dict[str, str], paths: list[str] | None, status: str, message: str) -> None:
    target = qualified_name(schema_name, "tb_mig_dag_dply_hist")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            for index, dag_name in enumerate(sources):
                path = paths[index] if paths and index < len(paths) else None
                cursor.execute(f"INSERT INTO {target} (airflow_id, dag_nm, dply_mthd_cd, dply_sts_cd, dply_file_path, dply_msg) VALUES (%s, %s, %s, %s, %s, %s)", (airflow_id(airflow), dag_name, text(method).upper(), status, path, message[:4000]))
        connection.commit()


def save_airflow(values: dict[str, Any], schema_name: str, qualified_name: Callable[[str, str], str], record: dict[str, object]) -> None:
    item_id = airflow_id(record["airflow_id"])
    name = text(record["airflow_nm"])
    method = text(record["dply_mthd_cd"]).upper()
    section = text(record["sec_sect_nm"])
    if not name or len(name) > 200 or method not in DEPLOY_METHODS or not section or len(section) > 100:
        raise ValueError("Airflow 입력값을 확인하십시오.")
    table = qualified_name(schema_name, "tb_mig_airflow")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM {table} WHERE airflow_id = %s", (item_id,))
            if cursor.fetchone() is None:
                cursor.execute(f"INSERT INTO {table} (airflow_id, airflow_nm, dply_mthd_cd, sec_sect_nm, active_yn) VALUES (%s, %s, %s, %s, %s)", (item_id, name, method, section, bool(record["active_yn"])))
            else:
                cursor.execute(f"UPDATE {table} SET airflow_nm = %s, dply_mthd_cd = %s, sec_sect_nm = %s, active_yn = %s, upd_dtm = GETDATE() WHERE airflow_id = %s", (name, method, section, bool(record["active_yn"]), item_id))
        connection.commit()


def render_airflow_management(values: dict[str, Any], schema_name: str, query_frame: Callable[..., pd.DataFrame], qualified_name: Callable[[str, str], str]) -> None:
    if not st.button("Airflow 정보 조회", icon=":material/search:") and "airflow_loaded" not in st.session_state:
        return
    st.session_state.airflow_loaded = True
    try:
        frame = airflow_frame(query_frame, values, schema_name, qualified_name)
    except Exception as error:
        st.error(f"Airflow 정보 조회 실패: {error}", icon=":material/error:")
        return
    st.dataframe(frame.rename(columns={"airflow_id": "Airflow ID", "airflow_nm": "Airflow명", "dply_mthd_cd": "배포방식", "sec_sect_nm": "Secrets 섹션명", "active_yn": "사용", "crt_dtm": "등록일시", "upd_dtm": "수정일시"}), hide_index=True)
    selected = st.selectbox("수정 대상", ["신규", *frame.airflow_id.tolist()])
    current = None if selected == "신규" else frame.loc[frame.airflow_id.eq(selected)].iloc[0]
    with st.form("airflow_form"):
        item_id = st.text_input("Airflow ID", value="" if current is None else text(current.airflow_id), disabled=current is not None)
        name = st.text_input("Airflow명", value="" if current is None else text(current.airflow_nm))
        method = st.selectbox("DAG 배포방식", DEPLOY_METHODS, index=0 if current is None else DEPLOY_METHODS.index(text(current.dply_mthd_cd).upper()))
        section = st.text_input("Secrets 섹션명", value="" if current is None else text(current.sec_sect_nm))
        active = st.toggle("사용", value=True if current is None else bool(current.active_yn))
        saved = st.form_submit_button("Airflow 저장", type="primary", icon=":material/save:")
    if saved:
        try:
            save_airflow(values, schema_name, qualified_name, {"airflow_id": item_id, "airflow_nm": name, "dply_mthd_cd": method, "sec_sect_nm": section, "active_yn": active})
            st.success("Airflow 정보를 저장했습니다.", icon=":material/check_circle:")
            st.session_state.airflow_loaded = False
            st.rerun()
        except Exception as error:
            st.error(f"Airflow 저장 실패: {error}", icon=":material/error:")

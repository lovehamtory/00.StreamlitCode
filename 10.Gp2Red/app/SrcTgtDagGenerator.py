from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAG_OUTPUT_ROOT = PROJECT_ROOT / "dag"
SCHEDULES = {"NONE": None, "DLY_0100": "0 1 * * *", "DLY_0200": "0 2 * * *", "DLY_0300": "0 3 * * *", "DLY_0400": "0 4 * * *", "DLY_0500": "0 5 * * *"}
AREA_DAG_TYPES = {"FULL_SRC_S3", "FULL_S3_TGT", "VALD_SRC_S3", "VALD_S3_TGT"}
TABLE_DAG_TYPES = {"INCR_SRC_S3", "INCR_S3_TGT", "INCR_ALL", "RELOAD_SRC_S3", "RELOAD_S3_TGT", "RELOAD_ALL"}


def text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def subject_code(value: object) -> str:
    code = text(value).upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,19}", code):
        raise ValueError("주제영역코드는 영문으로 시작하는 영문·숫자·밑줄 1~20자리여야 합니다.")
    return code


def dag_id(subject_area: object, dag_type: str, mapping_id: int | None = None) -> str:
    area = subject_code(subject_area).lower()
    kind = text(dag_type).upper()
    if kind in AREA_DAG_TYPES:
        return f"mig_{area}_{kind.lower()}"
    if kind in TABLE_DAG_TYPES:
        if not mapping_id or int(mapping_id) < 1:
            raise ValueError("테이블별 DAG에는 테이블매핑ID가 필요합니다.")
        return f"mig_{area}_{int(mapping_id)}_{kind.lower()}"
    raise ValueError("지원하지 않는 DAG 구분입니다.")


def parse_conditions(value: object) -> list[str]:
    if isinstance(value, list):
        return [text(item) for item in value if text(item)]
    raw = text(value)
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError("S3 병렬 조건은 JSON 문자열 배열이어야 합니다.") from error
    if not isinstance(parsed, list) or any(not text(item) for item in parsed):
        raise ValueError("S3 병렬 조건을 확인하십시오.")
    return [text(item) for item in parsed]


def generated_source(dag_name: str, dag_type: str, subject_area: str, mapping_id: int | None, parallelism: int, schedule: str | None, tags: list[str]) -> str:
    filter_clause = "sbj_area_cd = %s" if mapping_id is None else "mpg_id = %s"
    filter_value = subject_area if mapping_id is None else int(mapping_id)
    flow = text(dag_type).upper()
    load_filter = " AND load_sts_cd = 'FULL'" if flow.startswith("FULL_") else " AND load_sts_cd = 'INCR'" if flow.startswith("INCR_") else ""
    if flow in {"FULL_SRC_S3", "INCR_SRC_S3", "RELOAD_SRC_S3"}:
        steps = "s3_result = run_s3.expand(record=expand_parallel(records))\n    final_result = validate_src_s3.expand(record=s3_result)"
    elif flow in {"FULL_S3_TGT", "INCR_S3_TGT", "RELOAD_S3_TGT"}:
        steps = "ins_result = run_ins.expand(record=records)\n    final_result = validate_s3_tgt.expand(record=ins_result)"
    elif flow in {"INCR_ALL", "RELOAD_ALL"}:
        steps = "s3_result = run_s3.expand(record=expand_parallel(records))\n    src_validated = validate_src_s3.expand(record=s3_result)\n    ins_result = run_ins.expand(record=src_validated)\n    final_result = validate_s3_tgt.expand(record=ins_result)"
    elif flow == "VALD_SRC_S3":
        steps = "final_result = validate_src_s3.expand(record=records)"
    elif flow == "VALD_S3_TGT":
        steps = "final_result = validate_s3_tgt.expand(record=records)"
    else:
        raise ValueError("지원하지 않는 DAG 구분입니다.")
    source = f'''from __future__ import annotations

import importlib
import json
from datetime import datetime
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = {dag_name!r}
DAG_TYPE = {flow!r}
SUBJECT_AREA = {subject_area!r}
MAPPING_ID = {mapping_id!r}
MAP_FILTER_SQL = {filter_clause!r}
MAP_FILTER_VALUE = {filter_value!r}
MAX_PARALLEL = {int(parallelism)}
EXECUTOR_MODULE = Variable.get("mig_executor_module", default_var="")
METADATA_CONN_ID = Variable.get("mig_metadata_conn_id", default_var="")
METADATA_SCHEMA = Variable.get("mig_metadata_schema", default_var="mig_meta")

def metadata_hook() -> PostgresHook:
    if not METADATA_CONN_ID:
        raise RuntimeError("Airflow Variable mig_metadata_conn_id를 설정하십시오.")
    return PostgresHook(postgres_conn_id=METADATA_CONN_ID)

def meta_table(name: str) -> str:
    return METADATA_SCHEMA + "." + name

def mappings(dag_run_id: str) -> list[dict[str, Any]]:
    query = "SELECT mpg_id, prj_cd, sbj_area_cd, src_conn_id, src_sch_nm, src_tbl_nm, tgt_conn_id, tgt_sch_nm, tgt_tbl_nm, load_sts_cd, incr_basis_cd, incr_basis_col_nm, parl_mthd_cd, parl_cnd_arr, s3_stg_path, s3_rlt_path FROM " + meta_table("vw_mig_dag_tbl_mpg") + " WHERE " + MAP_FILTER_SQL + {load_filter!r} + " ORDER BY mpg_id"
    rows = metadata_hook().get_records(query, parameters=(MAP_FILTER_VALUE,))
    columns = ["mpg_id", "prj_cd", "sbj_area_cd", "src_conn_id", "src_sch_nm", "src_tbl_nm", "tgt_conn_id", "tgt_sch_nm", "tgt_tbl_nm", "load_sts_cd", "incr_basis_cd", "incr_basis_col_nm", "parl_mthd_cd", "parl_cnd_arr", "s3_stg_path", "s3_rlt_path"]
    return [dict(zip(columns, row)) | {{"dag_nm": DAG_ID, "dag_run_id": dag_run_id, "dag_type": DAG_TYPE}} for row in rows]

def parallel_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    for record in records:
        conditions = json.loads(record.get("parl_cnd_arr") or "[]") if record.get("parl_mthd_cd") == "WHERE" else []
        for condition in conditions or [None]:
            expanded.append(dict(record) | {{"src_where_cnd": condition}})
    return expanded

def write_log(record: dict[str, Any], step: str, status: str, message: str) -> None:
    now = datetime.now()
    metadata_hook().run("INSERT INTO " + meta_table("tb_mig_run_log") + " (dag_nm, dag_run_id, mpg_id, task_nm, wrk_dvsn_cd, load_mthd_cd, wrk_sts_cd, src_row_cnt, tgt_row_cnt, s3_byte_size, s3_mnf_path, sql_file_path, src_where_cnd, wrk_stt_dtm, wrk_end_dtm, wrk_elps_sec, wrk_msg) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", parameters=(DAG_ID, record["dag_run_id"], record.get("mpg_id"), step.lower(), step, record.get("load_sts_cd"), status, record.get("src_row_cnt"), record.get("tgt_row_cnt"), record.get("s3_byte_size"), record.get("s3_mnf_path"), record.get("sql_file_path"), record.get("src_where_cnd"), now, now if status in {{"SUCCESS", "FAILED"}} else None, record.get("wrk_elps_sec"), message))

def execute(record: dict[str, Any], step: str, label: str) -> dict[str, Any]:
    write_log(record, step, "RUNNING", label + " 시작")
    try:
        if not EXECUTOR_MODULE:
            raise RuntimeError("Airflow Variable mig_executor_module을 설정하십시오.")
        handler = getattr(importlib.import_module(EXECUTOR_MODULE), "run_" + step.lower(), None)
        if not callable(handler):
            raise RuntimeError(EXECUTOR_MODULE + ".run_" + step.lower() + " 실행기를 찾을 수 없습니다.")
        result = handler(dict(record))
        if isinstance(result, dict):
            record.update(result)
    except Exception as error:
        write_log(record, step, "FAILED", str(error))
        raise
    write_log(record, step, "SUCCESS", label + " 완료")
    return record

def write_dag_run(dag_run_id: str, status: str, message: str, declared_count: int = 0) -> None:
    hook = metadata_hook()
    summary = hook.get_first("SELECT COUNT(DISTINCT mpg_id), COUNT(DISTINCT CASE WHEN wrk_sts_cd = 'SUCCESS' THEN mpg_id END), COUNT(DISTINCT CASE WHEN wrk_sts_cd = 'RUNNING' THEN mpg_id END), COUNT(DISTINCT CASE WHEN wrk_sts_cd = 'FAILED' THEN mpg_id END) FROM " + meta_table("tb_mig_run_log") + " WHERE dag_nm = %s AND dag_run_id = %s", parameters=(DAG_ID, dag_run_id))
    map_cnt, suc_cnt, run_cnt, err_cnt = [int(value or 0) for value in summary]
    map_cnt = max(map_cnt, declared_count)
    existing = hook.get_first("SELECT dag_exec_id FROM " + meta_table("tb_mig_dag_run") + " WHERE dag_nm = %s AND dag_run_id = %s ORDER BY dag_exec_id DESC LIMIT 1", parameters=(DAG_ID, dag_run_id))
    if existing:
        hook.run("UPDATE " + meta_table("tb_mig_dag_run") + " SET map_cnt = %s, suc_cnt = %s, run_cnt = %s, err_cnt = %s, wrk_sts_cd = %s, wrk_end_dtm = CASE WHEN %s IN ('SUCCESS','FAILED') THEN GETDATE() ELSE NULL END, wrk_msg = %s WHERE dag_exec_id = %s", parameters=(map_cnt, suc_cnt, run_cnt, err_cnt, status, status, message, existing[0]))
    else:
        hook.run("INSERT INTO " + meta_table("tb_mig_dag_run") + " (dag_nm, dag_run_id, dag_dvsn_cd, map_cnt, suc_cnt, run_cnt, err_cnt, wrk_sts_cd, wrk_stt_dtm, wrk_end_dtm, wrk_elps_sec, wrk_msg) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, GETDATE(), CASE WHEN %s IN ('SUCCESS','FAILED') THEN GETDATE() ELSE NULL END, NULL, %s)", parameters=(DAG_ID, dag_run_id, DAG_TYPE, map_cnt, suc_cnt, run_cnt, err_cnt, status, status, message))

@dag(dag_id=DAG_ID, start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"), schedule={schedule!r}, catchup=False, max_active_tasks=MAX_PARALLEL, tags={tags!r})
def migration_dag() -> None:
    @task
    def prepare(**context: Any) -> list[dict[str, Any]]:
        records = mappings(context["dag_run"].run_id)
        if not records:
            raise RuntimeError("생성 조건에 해당하는 활성 테이블매핑이 없습니다.")
        write_dag_run(context["dag_run"].run_id, "RUNNING", "DAG 시작", len(records))
        return records

    @task
    def expand_parallel(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return parallel_records(records)

    @task
    def run_s3(record: dict[str, Any]) -> dict[str, Any]:
        return execute(record, "S3", "원천 S3 적재")

    @task(max_active_tis_per_dag=1)
    def run_ins(record: dict[str, Any]) -> dict[str, Any]:
        return execute(record, "INS", "S3 대상 적재")

    @task
    def validate_src_s3(record: dict[str, Any]) -> dict[str, Any]:
        return execute(record, "VALIDATE_SRC_S3", "원천 S3 검증")

    @task
    def validate_s3_tgt(record: dict[str, Any]) -> dict[str, Any]:
        return execute(record, "VALIDATE_S3_TGT", "S3 대상 검증")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def finalize(**context: Any) -> None:
        instances = context["dag_run"].get_task_instances()
        failed = [item.task_id for item in instances if str(item.state).upper().endswith("FAILED")]
        status = "FAILED" if failed else "SUCCESS"
        write_dag_run(context["dag_run"].run_id, status, "실패 태스크: " + ", ".join(failed) if failed else "정상 완료")
        if failed:
            raise RuntimeError("실패 태스크: " + ", ".join(failed))

    records = prepare()
    {steps}
    finalize_task = finalize()
    finalize_task.set_upstream(final_result)

migration_dag()
'''
    return source


def area_dag_sources(area: str, settings: dict[str, object]) -> dict[str, str]:
    code = subject_code(area)
    s3_parallel = int(settings.get("s3_maximum", 1) or 1)
    ins_parallel = int(settings.get("ins_maximum", 1) or 1)
    return {
        dag_id(code, "FULL_SRC_S3"): generated_source(dag_id(code, "FULL_SRC_S3"), "FULL_SRC_S3", code, None, s3_parallel, None, ["mig", code.lower(), "full", "src_s3"]),
        dag_id(code, "FULL_S3_TGT"): generated_source(dag_id(code, "FULL_S3_TGT"), "FULL_S3_TGT", code, None, ins_parallel, None, ["mig", code.lower(), "full", "s3_tgt"]),
        dag_id(code, "VALD_SRC_S3"): generated_source(dag_id(code, "VALD_SRC_S3"), "VALD_SRC_S3", code, None, s3_parallel, None, ["mig", code.lower(), "validation", "src_s3"]),
        dag_id(code, "VALD_S3_TGT"): generated_source(dag_id(code, "VALD_S3_TGT"), "VALD_S3_TGT", code, None, ins_parallel, None, ["mig", code.lower(), "validation", "s3_tgt"]),
    }


def table_dag_sources(row: dict[str, Any], settings: dict[str, object], purpose: str) -> dict[str, str]:
    area = subject_code(row["sbj_area_cd"])
    map_id = int(row["mpg_id"])
    prefix = "INCR" if purpose == "INCR" else "RELOAD"
    schedule = SCHEDULES.get(text(settings.get("incr_schedule", "NONE")).upper()) if purpose == "INCR" else None
    s3_parallel = int(settings.get("s3_maximum", 1) or 1)
    ins_parallel = int(settings.get("ins_maximum", 1) or 1)
    return {
        dag_id(area, f"{prefix}_SRC_S3", map_id): generated_source(dag_id(area, f"{prefix}_SRC_S3", map_id), f"{prefix}_SRC_S3", area, map_id, s3_parallel, schedule, ["mig", area.lower(), str(map_id), prefix.lower(), "src_s3"]),
        dag_id(area, f"{prefix}_S3_TGT", map_id): generated_source(dag_id(area, f"{prefix}_S3_TGT", map_id), f"{prefix}_S3_TGT", area, map_id, ins_parallel, None, ["mig", area.lower(), str(map_id), prefix.lower(), "s3_tgt"]),
        dag_id(area, f"{prefix}_ALL", map_id): generated_source(dag_id(area, f"{prefix}_ALL", map_id), f"{prefix}_ALL", area, map_id, s3_parallel, schedule, ["mig", area.lower(), str(map_id), prefix.lower(), "integrated"]),
    }


def save_dag_files(sources: dict[str, str]) -> list[Path]:
    DAG_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for name, source in sources.items():
        path = DAG_OUTPUT_ROOT / f"{name}.py"
        path.write_text(source, encoding="utf-8")
        paths.append(path)
    return paths


def setting_rows(areas: pd.DataFrame, values: dict[str, Any], schema_name: str, query_frame: Callable[..., pd.DataFrame], qualified: Callable[[str, str], str]) -> pd.DataFrame:
    query = f'''SELECT A.sbj_area_cd, A.sbj_area_nm,
                       MAX(CASE WHEN D.dag_dvsn_cd = 'FULL_SRC_S3' THEN D.dflt_parl_cnt END) AS s3_default,
                       MAX(CASE WHEN D.dag_dvsn_cd = 'FULL_SRC_S3' THEN D.max_parl_cnt END) AS s3_maximum,
                       MAX(CASE WHEN D.dag_dvsn_cd = 'FULL_S3_TGT' THEN D.dflt_parl_cnt END) AS ins_default,
                       MAX(CASE WHEN D.dag_dvsn_cd = 'FULL_S3_TGT' THEN D.max_parl_cnt END) AS ins_maximum,
                       MAX(CASE WHEN D.dag_dvsn_cd = 'INCR_SRC_S3' THEN D.schd_cd END) AS incr_schedule
                  FROM {qualified(schema_name, 'tb_mig_sbj_area')} A
                  LEFT JOIN {qualified(schema_name, 'tb_mig_sbj_dag_mpg')} D ON D.sbj_area_cd = A.sbj_area_cd AND D.active_yn = TRUE
                 WHERE A.active_yn = TRUE
                 GROUP BY A.sbj_area_cd, A.sbj_area_nm
                 ORDER BY A.sbj_area_cd'''
    result = query_frame(values, query)
    for column, default in (("s3_default", 1), ("s3_maximum", 1), ("ins_default", 1), ("ins_maximum", 1)):
        result[column] = result[column].fillna(default).astype(int)
    result["incr_schedule"] = result["incr_schedule"].fillna("NONE")
    return result


def save_settings(values: dict[str, Any], schema_name: str, connect: Callable[..., Any], qualified: Callable[[str, str], str], area: str, settings: dict[str, object]) -> None:
    if any(int(settings[item]) < 1 for item in ("s3_default", "s3_maximum", "ins_default", "ins_maximum")):
        raise ValueError("병렬도는 1 이상이어야 합니다.")
    if int(settings["s3_default"]) > int(settings["s3_maximum"]) or int(settings["ins_default"]) > int(settings["ins_maximum"]):
        raise ValueError("최대 병렬도는 기본 병렬도보다 작을 수 없습니다.")
    schedule = text(settings["incr_schedule"]).upper()
    if schedule not in SCHEDULES:
        raise ValueError("증분 일정을 확인하십시오.")
    records = (("FULL_SRC_S3", settings["s3_default"], settings["s3_maximum"], "NONE"), ("FULL_S3_TGT", settings["ins_default"], settings["ins_maximum"], "NONE"), ("INCR_SRC_S3", settings["s3_default"], settings["s3_maximum"], schedule))
    with connect(values) as connection:
        with connection.cursor() as cursor:
            for kind, default_parallel, maximum_parallel, schedule_code in records:
                cursor.execute(f"DELETE FROM {qualified(schema_name, 'tb_mig_sbj_dag_mpg')} WHERE sbj_area_cd = %s AND dag_dvsn_cd = %s", (area, kind))
                cursor.execute(f"INSERT INTO {qualified(schema_name, 'tb_mig_sbj_dag_mpg')} (sbj_area_cd, dag_dvsn_cd, dflt_parl_cnt, max_parl_cnt, schd_cd, active_yn) VALUES (%s, %s, %s, %s, %s, TRUE)", (area, kind, int(default_parallel), int(maximum_parallel), schedule_code))
        connection.commit()


def check_sources(sources: dict[str, str]) -> None:
    for name, source in sources.items():
        compile(source, f"{name}.py", "exec")


def render_dag_generator(areas: pd.DataFrame, maps: pd.DataFrame, values: dict[str, Any], schema_name: str, query_frame: Callable[..., pd.DataFrame], connect: Callable[..., Any], qualified: Callable[[str, str], str]) -> None:
    if areas.empty:
        st.info("주제영역을 먼저 등록하십시오.", icon=":material/info:")
        return
    selected_area = st.selectbox("주제영역", areas.sbj_area_cd.tolist(), format_func=lambda item: f"{item} · {text(areas.loc[areas.sbj_area_cd.eq(item)].iloc[0].sbj_area_nm) or '미정'}")
    current = areas.loc[areas.sbj_area_cd.eq(selected_area)].iloc[0]
    try:
        settings_frame = setting_rows(areas, values, schema_name, query_frame, qualified)
        saved = settings_frame.loc[settings_frame.sbj_area_cd.eq(selected_area)].iloc[0]
    except Exception as error:
        st.error(f"DAG 설정 조회 실패: {error}", icon=":material/error:")
        return
    with st.form("subject_dag_settings"):
        left, right = st.columns(2)
        with left:
            s3_default = st.number_input("S3 기본 병렬", min_value=1, value=int(saved.s3_default))
            s3_maximum = st.number_input("S3 최대 병렬", min_value=1, value=int(saved.s3_maximum))
        with right:
            ins_default = st.number_input("대상 적재 기본 병렬", min_value=1, value=int(saved.ins_default))
            ins_maximum = st.number_input("대상 적재 최대 병렬", min_value=1, value=int(saved.ins_maximum))
        options = list(SCHEDULES)
        incr_schedule = st.selectbox("증분 S3 일정", options, index=options.index(text(saved.incr_schedule).upper()) if text(saved.incr_schedule).upper() in options else 0)
        saved_clicked = st.form_submit_button("DAG 설정 저장", type="primary", icon=":material/save:")
    settings = {"s3_default": s3_default, "s3_maximum": s3_maximum, "ins_default": ins_default, "ins_maximum": ins_maximum, "incr_schedule": incr_schedule}
    if saved_clicked:
        try:
            save_settings(values, schema_name, connect, qualified, selected_area, settings)
            st.success("주제영역 DAG 설정을 저장했습니다.", icon=":material/check_circle:")
        except Exception as error:
            st.error(f"DAG 설정 저장 실패: {error}", icon=":material/error:")
            return
    mode = st.segmented_control("생성 유형", ["주제영역 전체·검증", "테이블별 증분", "일회성 재적재"], default="주제영역 전체·검증", label_visibility="collapsed")
    if mode == "주제영역 전체·검증":
        sources = area_dag_sources(selected_area, settings)
    else:
        candidates = maps.loc[maps.sbj_area_cd.map(text).str.upper().eq(selected_area)].copy()
        if mode == "테이블별 증분":
            candidates = candidates.loc[candidates.load_sts_cd.map(text).str.upper().eq("INCR")]
        if candidates.empty:
            st.info("조건에 맞는 테이블매핑이 없습니다.", icon=":material/info:")
            return
        selected_map = st.selectbox("테이블매핑", candidates.mpg_id.tolist(), format_func=lambda item: f"{item} · {candidates.loc[candidates.mpg_id.eq(item)].iloc[0].src_sch_nm}.{candidates.loc[candidates.mpg_id.eq(item)].iloc[0].src_tbl_nm} → {candidates.loc[candidates.mpg_id.eq(item)].iloc[0].tgt_sch_nm}.{candidates.loc[candidates.mpg_id.eq(item)].iloc[0].tgt_tbl_nm}")
        row = candidates.loc[candidates.mpg_id.eq(selected_map)].iloc[0].to_dict()
        if mode == "일회성 재적재":
            st.caption("현재 FULL·INCR 기본 적재상태는 바꾸지 않습니다. Airflow 실행 시 전체 또는 WHERE 병렬 조건은 dag_run.conf로 지정합니다.")
        sources = table_dag_sources(row, settings, "INCR" if mode == "테이블별 증분" else "RELOAD")
    try:
        check_sources(sources)
        st.success("생성 대상 DAG Python 문법을 확인했습니다.", icon=":material/check_circle:")
    except Exception as error:
        st.error(f"DAG 문법 검증 실패: {error}", icon=":material/error:")
        return
    selected_name = st.selectbox("DAG 미리보기", list(sources))
    st.code(sources[selected_name], language="python")
    if st.button("DAG 파일 생성", type="primary", icon=":material/terminal:"):
        paths = save_dag_files(sources)
        st.success("생성 완료: " + ", ".join(path.name for path in paths), icon=":material/check_circle:")
    st.download_button("선택 DAG 다운로드", data=sources[selected_name], file_name=f"{selected_name}.py", mime="text/x-python", icon=":material/download:")

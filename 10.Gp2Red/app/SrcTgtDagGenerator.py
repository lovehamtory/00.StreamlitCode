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
AREA_DAG_TYPES = {"FULL_SRC_S3", "FULL_S3_TGT", "FULL_ALL", "VALD_SRC_S3", "VALD_S3_TGT"}
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
        steps = "prepared = reset_full_s3.expand(record=records)\n    s3_result = run_s3.expand(record=expand_parallel(prepared))\n    src_validated = validate_src_s3.expand(record=s3_result)\n    final_result = cleanup_increment_s3(src_validated)"
    elif flow in {"FULL_S3_TGT", "INCR_S3_TGT", "RELOAD_S3_TGT"}:
        steps = "ins_result = run_ins.expand(record=records)\n    final_result = validate_s3_tgt.expand(record=ins_result)"
    elif flow in {"FULL_ALL", "INCR_ALL", "RELOAD_ALL"}:
        steps = "prepared = reset_full_s3.expand(record=records)\n    s3_result = run_s3.expand(record=expand_parallel(prepared))\n    src_validated = validate_src_s3.expand(record=s3_result)\n    cleanup = cleanup_increment_s3(src_validated)\n    ins_result = run_ins.expand(record=src_validated)\n    ins_result.set_upstream(cleanup)\n    final_result = validate_s3_tgt.expand(record=ins_result)"
    elif flow == "VALD_SRC_S3":
        steps = "final_result = validate_src_s3.expand(record=records)"
    elif flow == "VALD_S3_TGT":
        steps = "final_result = validate_s3_tgt.expand(record=records)"
    else:
        raise ValueError("지원하지 않는 DAG 구분입니다.")
    source = f'''from __future__ import annotations

import importlib
import json
import re
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
    query = "SELECT mpg_id, prj_cd, sbj_area_cd, src_conn_id, src_sch_nm, src_tbl_nm, tgt_conn_id, tgt_sch_nm, tgt_tbl_nm, load_sts_cd, sys_col_nm_arr, sys_col_fmt_cd, incr_mthd_cd, src_incr_col_nm_arr, parl_mthd_cd, parl_cnd_arr, src_ext_sql, tgt_load_sql, s3_stg_path, s3_rlt_path FROM " + meta_table("vw_mig_dag_tbl_mpg") + " WHERE " + MAP_FILTER_SQL + {load_filter!r} + " ORDER BY mpg_id"
    rows = metadata_hook().get_records(query, parameters=(MAP_FILTER_VALUE,))
    columns = ["mpg_id", "prj_cd", "sbj_area_cd", "src_conn_id", "src_sch_nm", "src_tbl_nm", "tgt_conn_id", "tgt_sch_nm", "tgt_tbl_nm", "load_sts_cd", "sys_col_nm_arr", "sys_col_fmt_cd", "incr_mthd_cd", "src_incr_col_nm_arr", "parl_mthd_cd", "parl_cnd_arr", "src_ext_sql", "tgt_load_sql", "s3_stg_path", "s3_rlt_path"]
    return [dict(zip(columns, row)) | {{"dag_nm": DAG_ID, "dag_run_id": dag_run_id, "dag_type": DAG_TYPE}} for row in rows]

def quote_identifier(value: object) -> str:
    name = str(value or "").strip()
    if not name or "\\x00" in name:
        raise RuntimeError("대상 식별자를 확인하십시오.")
    return '"' + name.replace('"', '""') + '"'

def column_mappings(record: dict[str, Any]) -> list[dict[str, Any]]:
    query = "SELECT src_col_nm, tgt_col_nm, tgt_data_type, col_mpg_mthd_cd, tgt_expr, dflt_expr, src_ref_col_nm_arr FROM " + meta_table("tb_mig_col_mpg") + " WHERE mpg_id = %s AND active_yn = TRUE ORDER BY col_ord"
    columns = ["src_col_nm", "tgt_col_nm", "tgt_data_type", "col_mpg_mthd_cd", "tgt_expr", "dflt_expr", "src_ref_col_nm_arr"]
    return [dict(zip(columns, row)) for row in metadata_hook().get_records(query, parameters=(record["mpg_id"],))]

def source_target_increment_columns(record: dict[str, Any], mappings: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    source_keys = json.loads(record.get("src_incr_col_nm_arr") or "[]")
    if not isinstance(source_keys, list) or not source_keys:
        raise RuntimeError("원천 증분 컬럼명이 없습니다.")
    by_source = {{str(row.get("src_col_nm") or "").upper(): row for row in mappings}}
    targets: list[str] = []
    for source_key in source_keys:
        item = by_source.get(str(source_key).upper())
        if not item or not str(item.get("tgt_col_nm") or "").strip():
            raise RuntimeError("원천 증분 컬럼의 대상 컬럼매핑이 없습니다: " + str(source_key))
        targets.append(str(item["tgt_col_nm"]))
    return [str(value) for value in source_keys], targets

def source_layout_columns(mappings: list[dict[str, Any]]) -> list[str]:
    result: list[str] = []
    for row in mappings:
        name = str(row.get("src_col_nm") or "").strip()
        if name and name.upper() not in {{item.upper() for item in result}}:
            result.append(name)
    return result

def source_value(value: object, format_code: object) -> str:
    raw = str(value or "").strip().replace("'", "''")
    code = str(format_code or "").upper()
    if not raw:
        raise RuntimeError("증분 실행 기준값이 없습니다.")
    if code == "YYYYMMDD":
        return "'" + raw + "'"
    if code == "YYYYMMDDHH24MISS":
        return "TO_TIMESTAMP('" + raw + "', 'YYYYMMDDHH24MISS')"
    if code == "DATE":
        return "TO_DATE('" + raw + "', 'YYYY-MM-DD')"
    if code == "TIMESTAMP":
        return "TIMESTAMP '" + raw + "'"
    raise RuntimeError("시스템 컬럼 데이터 형식을 확인하십시오.")

def s3_run_path(record: dict[str, Any], run_conf: dict[str, Any]) -> None:
    work_date = str(run_conf.get("wrk_dt") or datetime.now().strftime("%Y%m%d")).replace("-", "")
    if not re.fullmatch(r"[0-9]{{8}}", work_date):
        raise RuntimeError("작업일자는 YYYYMMDD 형식이어야 합니다.")
    base = str(record.get("s3_stg_path") or "").rstrip("/")
    table_key = str(record.get("s3_rlt_path") or "").strip("/")
    if not base.startswith("s3://") or not table_key:
        raise RuntimeError("S3 기준경로를 확인하십시오.")
    load_folder = "full" if str(record.get("load_sts_cd") or "").upper() == "FULL" else "incr"
    run_label = re.sub(r"[^A-Za-z0-9._=-]+", "_", str(record.get("dag_run_id") or "").strip())
    if not run_label:
        raise RuntimeError("DAG 실행ID를 확인하십시오.")
    root = base + "/" + load_folder + "/" + table_key
    record["s3_work_dt"] = work_date
    record["s3_load_path"] = root if load_folder == "full" else root + "/wrk_dt=" + work_date + "/run_id=" + run_label
    record["s3_cleanup_prefix"] = root
    record["s3_cleanup_before_write"] = load_folder == "full"
    record["s3_retention_days"] = 31 if load_folder == "incr" else 0

def source_extract_plan(record: dict[str, Any], run_conf: dict[str, Any]) -> dict[str, Any]:
    mappings = column_mappings(record)
    source_table = quote_identifier(record["src_sch_nm"]) + "." + quote_identifier(record["src_tbl_nm"])
    custom_sql = str(record.get("src_ext_sql") or "").strip()
    if custom_sql:
        if "__SRC_WHERE_CND__" not in custom_sql:
            raise RuntimeError("수정 원천 추출 SQL에는 __SRC_WHERE_CND__ 치환값이 필요합니다.")
    custom_condition = str(run_conf.get("src_where_cnd") or "").strip()
    if any(token in custom_condition for token in (";", "--", "/*", "*/")):
        raise RuntimeError("원천 조회조건에 세미콜론 또는 SQL 주석을 입력할 수 없습니다.")
    if str(record.get("load_sts_cd") or "").upper() == "INCR" and not custom_condition:
        source_keys, _ = source_target_increment_columns(record, mappings)
        system_columns = json.loads(record.get("sys_col_nm_arr") or "[]")
        if not isinstance(system_columns, list) or not system_columns:
            raise RuntimeError("시스템 컬럼명이 없습니다.")
        criterion = source_value(run_conf.get("sys_ref_val"), record.get("sys_col_fmt_cd"))
        system_predicate = " OR ".join("I." + quote_identifier(column) + " >= " + criterion for column in system_columns)
        outer_keys = "(" + ", ".join("S." + quote_identifier(column) for column in source_keys) + ")" if len(source_keys) > 1 else "S." + quote_identifier(source_keys[0])
        inner_keys = "(" + ", ".join("I." + quote_identifier(column) for column in source_keys) + ")" if len(source_keys) > 1 else "I." + quote_identifier(source_keys[0])
        custom_condition = outer_keys + " IN (SELECT " + inner_keys + " FROM " + source_table + " AS I WHERE (" + system_predicate + "))"
    record["src_base_cnd"] = custom_condition or None
    if not custom_sql:
        source_columns = source_layout_columns(mappings)
        if not source_columns:
            raise RuntimeError("이관 SQL 또는 원천컬럼 매핑이 필요합니다.")
        custom_sql = "SELECT " + ", ".join("S." + quote_identifier(column) + " AS " + quote_identifier(column) for column in source_columns) + " FROM " + source_table + " AS S WHERE __SRC_WHERE_CND__"
    record["src_extract_sql"] = custom_sql
    s3_run_path(record, run_conf)
    return record

def target_column_value(row: dict[str, Any]) -> str:
    method = str(row.get("col_mpg_mthd_cd") or "MOVE").upper()
    expression = str(row.get("tgt_expr") or "").strip()
    source_name = str(row.get("src_col_nm") or "").strip()
    if expression:
        value = expression
    elif method == "NULL":
        data_type = str(row.get("tgt_data_type") or "").strip()
        if not data_type:
            raise RuntimeError("NULL 컬럼매핑에는 대상 데이터타입이 필요합니다.")
        value = "CAST(NULL AS " + data_type + ")"
    elif source_name:
        value = "S." + quote_identifier(source_name)
    elif method in {{"CONST", "EXPR"}}:
        raise RuntimeError(method + " 컬럼매핑에는 이행 SQL식이 필요합니다.")
    elif method == "MOVE":
        raise RuntimeError("MOVE 컬럼매핑에는 원천컬럼명이 필요합니다.")
    else:
        raise RuntimeError("컬럼매핑 방식을 확인하십시오.")
    default = str(row.get("dflt_expr") or "").strip()
    return "COALESCE(" + value + ", " + default + ")" if default else value

def target_load_plan(record: dict[str, Any]) -> dict[str, Any]:
    mappings = column_mappings(record)
    if not mappings:
        raise RuntimeError("대상 적재 컬럼매핑이 없습니다.")
    target = quote_identifier(record["tgt_sch_nm"]) + "." + quote_identifier(record["tgt_tbl_nm"])
    stage = str(record.get("tgt_stage_tbl") or "__MIG_STAGE__")
    custom_sql = str(record.get("tgt_load_sql") or "").strip()
    if custom_sql:
        if "__MIG_STAGE__" not in custom_sql:
            raise RuntimeError("수정 대상 적재 SQL에는 __MIG_STAGE__ 치환값이 필요합니다.")
        record["tgt_load_sql"] = custom_sql.replace("__TGT_TABLE__", target)
        record["tgt_stage_placeholder"] = "__MIG_STAGE__"
        return record
    target_columns = ", ".join(quote_identifier(row["tgt_col_nm"]) for row in mappings)
    select_columns = [target_column_value(row) for row in mappings]
    insert_sql = "INSERT INTO " + target + " (" + target_columns + ") SELECT " + ", ".join(select_columns) + " FROM " + stage + " AS S"
    if str(record.get("load_sts_cd") or "").upper() == "FULL":
        delete_sql = "TRUNCATE TABLE " + target
    else:
        source_keys, keys = source_target_increment_columns(record, mappings)
        by_source = {{str(row.get("src_col_nm") or "").upper(): row for row in mappings}}
        staged_keys = ", ".join(target_column_value(by_source[str(source_key).upper()]) + " AS " + quote_identifier(target_key) for source_key, target_key in zip(source_keys, keys))
        key_columns = ", ".join(quote_identifier(key) for key in keys)
        delete_sql = "DELETE FROM " + target + " AS T USING (SELECT DISTINCT " + staged_keys + " FROM " + stage + " AS S) AS S WHERE " + " AND ".join("T." + quote_identifier(key) + " = S." + quote_identifier(key) for key in keys)
    record["tgt_load_sql"] = (delete_sql + ";\\n" if delete_sql else "") + insert_sql + ";"
    record["tgt_stage_placeholder"] = "__MIG_STAGE__"
    return record

def parallel_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    for record in records:
        conditions = json.loads(record.get("parl_cnd_arr") or "[]") if record.get("parl_mthd_cd") == "WHERE" else []
        for sequence, condition in enumerate(conditions or [None], start=1):
            actual = " AND ".join("(" + item + ")" for item in (record.get("src_base_cnd"), condition) if item)
            expanded.append(dict(record) | {{"parl_seq": sequence, "src_parl_cnd": condition, "src_where_cnd": actual or None}})
    return expanded

def source_sql(record: dict[str, Any]) -> dict[str, Any]:
    query = str(record.get("src_extract_sql") or "").strip()
    if not query:
        raise RuntimeError("원천 추출 SQL이 없습니다.")
    condition = str(record.get("src_where_cnd") or "").strip()
    if "__SRC_WHERE_CND__" in query:
        record["src_extract_sql"] = query.replace("__SRC_WHERE_CND__", condition or "1=1")
    else:
        record["src_extract_sql"] = query + (" WHERE " + condition if condition else "")
    return record

def write_s3_manifest(record: dict[str, Any]) -> dict[str, Any]:
    required = ("s3_mnf_path", "s3_data_path")
    missing = [name for name in required if not str(record.get(name) or "").strip()]
    if missing:
        raise RuntimeError("S3 실행기는 매니페스트 필수값을 반환해야 합니다: " + ", ".join(missing))
    hook = metadata_hook()
    table = meta_table("tb_mig_s3_manf")
    key = (record["mpg_id"], DAG_ID, record["dag_run_id"], int(record.get("parl_seq") or 1))
    existing = hook.get_first("SELECT s3_manf_id FROM " + table + " WHERE mpg_id = %s AND dag_nm = %s AND dag_run_id = %s AND parl_seq = %s ORDER BY s3_manf_id DESC LIMIT 1", parameters=key)
    values = (record.get("s3_mnf_path"), record.get("s3_data_path"), record.get("src_row_cnt"), record.get("s3_row_cnt"), record.get("s3_byte_size"), record.get("src_where_cnd"))
    if existing:
        hook.run("UPDATE " + table + " SET s3_mnf_path = %s, s3_data_path = %s, src_row_cnt = %s, s3_row_cnt = %s, s3_byte_size = %s, src_where_cnd = %s, vald_sts_cd = 'PENDING', ins_sts_cd = 'PENDING', upd_dtm = GETDATE() WHERE s3_manf_id = %s", parameters=values + (existing[0],))
        record["s3_manf_id"] = int(existing[0])
    else:
        hook.run("INSERT INTO " + table + " (mpg_id, dag_nm, dag_run_id, parl_seq, src_where_cnd, s3_mnf_path, s3_data_path, load_mthd_cd, src_row_cnt, s3_row_cnt, s3_byte_size) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", parameters=key + (record.get("src_where_cnd"), record["s3_mnf_path"], record["s3_data_path"], record.get("load_sts_cd"), record.get("src_row_cnt"), record.get("s3_row_cnt"), record.get("s3_byte_size")))
        created = hook.get_first("SELECT s3_manf_id FROM " + table + " WHERE mpg_id = %s AND dag_nm = %s AND dag_run_id = %s AND parl_seq = %s ORDER BY s3_manf_id DESC LIMIT 1", parameters=key)
        if not created:
            raise RuntimeError("S3 매니페스트 식별자를 확인할 수 없습니다.")
        record["s3_manf_id"] = int(created[0])
    return record

def update_manifest_status(record: dict[str, Any], column: str, status: str) -> None:
    identifiers = [int(value) for value in record.get("s3_manf_ids", [])] or ([int(record["s3_manf_id"])] if record.get("s3_manf_id") else [])
    if not identifiers:
        raise RuntimeError("S3 매니페스트 식별자가 없습니다.")
    if column not in {{"vald_sts_cd", "ins_sts_cd"}} or status not in {{"SUCCESS", "FAILED"}}:
        raise RuntimeError("매니페스트 상태값을 확인하십시오.")
    placeholders = ", ".join(["%s"] * len(identifiers))
    metadata_hook().run("UPDATE " + meta_table("tb_mig_s3_manf") + " SET " + column + " = %s, upd_dtm = GETDATE() WHERE s3_manf_id IN (" + placeholders + ")", parameters=(status, *identifiers))

def source_dag_name() -> str:
    return DAG_ID.replace("_s3_tgt", "_src_s3")

def bind_source_manifests(records: list[dict[str, Any]], source_run_id: object) -> list[dict[str, Any]]:
    run_id = str(source_run_id or "").strip()
    if not run_id:
        raise RuntimeError("S3→대상 DAG 실행에는 dag_run.conf.source_dag_run_id가 필요합니다.")
    rows = metadata_hook().get_records("SELECT s3_manf_id, mpg_id, s3_mnf_path, s3_data_path, src_where_cnd FROM " + meta_table("tb_mig_s3_manf") + " WHERE dag_nm = %s AND dag_run_id = %s AND vald_sts_cd = 'SUCCESS' AND ins_sts_cd IN ('PENDING','FAILED') ORDER BY mpg_id, parl_seq", parameters=(source_dag_name(), run_id))
    grouped: dict[int, list[dict[str, Any]]] = {{}}
    for row in rows:
        item = dict(zip(["s3_manf_id", "mpg_id", "s3_mnf_path", "s3_data_path", "src_where_cnd"], row))
        grouped.setdefault(int(item["mpg_id"]), []).append(item)
    result: list[dict[str, Any]] = []
    for record in records:
        manifests = grouped.get(int(record["mpg_id"]), [])
        if not manifests:
            continue
        result.append(dict(record) | {{"source_dag_run_id": run_id, "s3_manifests": manifests, "s3_manf_ids": [item["s3_manf_id"] for item in manifests]}})
    if not result:
        raise RuntimeError("검증 완료된 S3 매니페스트가 없습니다.")
    return result

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
        run_conf = context["dag_run"].conf or {{}}
        if DAG_TYPE in {{"FULL_SRC_S3", "FULL_ALL", "INCR_SRC_S3", "RELOAD_SRC_S3", "INCR_ALL", "RELOAD_ALL", "VALD_SRC_S3"}}:
            records = [source_extract_plan(record, run_conf) for record in records]
        if DAG_TYPE in {"FULL_S3_TGT", "INCR_S3_TGT", "RELOAD_S3_TGT"}:
            records = bind_source_manifests(records, run_conf.get("source_dag_run_id"))
        if not records:
            raise RuntimeError("생성 조건에 해당하는 활성 테이블매핑이 없습니다.")
        write_dag_run(context["dag_run"].run_id, "RUNNING", "DAG 시작", len(records))
        return records

    @task
    def expand_parallel(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return parallel_records(records)

    @task
    def run_s3(record: dict[str, Any]) -> dict[str, Any]:
        return write_s3_manifest(execute(source_sql(record), "S3", "원천 S3 적재"))

    @task
    def reset_full_s3(record: dict[str, Any]) -> dict[str, Any]:
        if bool(record.get("s3_cleanup_before_write")):
            return execute(record, "S3_RESET", "S3 FULL 기준본 삭제")
        return record

    @task
    def cleanup_increment_s3(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        completed: list[dict[str, Any]] = []
        seen: set[int] = set()
        for record in records:
            mapping_id = int(record["mpg_id"])
            if mapping_id in seen:
                continue
            seen.add(mapping_id)
            if int(record.get("s3_retention_days") or 0) > 0:
                completed.append(execute(record, "S3_CLEANUP", "S3 증분 기준본 정리"))
        return completed

    @task(max_active_tis_per_dag=1)
    def run_ins(record: dict[str, Any]) -> dict[str, Any]:
        try:
            result = execute(target_load_plan(record), "INS", "S3 대상 적재")
            update_manifest_status(result, "ins_sts_cd", "SUCCESS")
            return result
        except Exception:
            update_manifest_status(record, "ins_sts_cd", "FAILED")
            raise

    @task
    def validate_src_s3(record: dict[str, Any]) -> dict[str, Any]:
        try:
            result = execute(record, "VALIDATE_SRC_S3", "원천 S3 검증")
            update_manifest_status(result, "vald_sts_cd", "SUCCESS")
            return result
        except Exception:
            update_manifest_status(record, "vald_sts_cd", "FAILED")
            raise

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
        dag_id(code, "FULL_ALL"): generated_source(dag_id(code, "FULL_ALL"), "FULL_ALL", code, None, s3_parallel, None, ["mig", code.lower(), "full", "integrated"]),
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

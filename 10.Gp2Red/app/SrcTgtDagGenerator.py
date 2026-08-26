from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


DAG_OUTPUT_ROOT = Path(__file__).parent.parent / "dag"


def text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def dag_id(sbj_area_cd: str, dag_dvsn_cd: str) -> str:
    code = text(sbj_area_cd).upper()
    division = text(dag_dvsn_cd).upper()
    suffix = {"S3": "s3", "INS": "ins", "FULL_CTL": "full_ctl", "INCR_CTL": "incr_ctl"}.get(division)
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,7}", code) or suffix is None:
        raise ValueError("주제영역코드 또는 DAG구분코드를 확인하십시오.")
    return f"mig_{code.lower()}_{suffix}"


def project_dag_id(project_code: str, phase: str = "FULL") -> str:
    code = text(project_code).upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,99}", code):
        raise ValueError("프로젝트코드를 확인하십시오.")
    normalized_phase = text(phase).upper()
    if normalized_phase not in {"FULL", "INCR"}:
        raise ValueError("프로젝트 실행구분은 FULL 또는 INCR이어야 합니다.")
    return f"mig_{code.lower()}_{normalized_phase.lower()}_orch"


COMMON_SOURCE = '''from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg
from airflow.hooks.base import BaseHook
from psycopg.rows import dict_row

META_CONN_ID = os.getenv("MIG_META_CONN_ID", "TGT_RED")
DAG_ROOT = Path(__file__).parent
LOG_ROOT = Path(os.getenv("MIG_LOG_ROOT", str(DAG_ROOT.parent / "log")))
SQL_ROOT = Path(os.getenv("MIG_SQL_ROOT", str(DAG_ROOT.parent / "sql")))
EXECUTOR_MODULE = os.getenv("MIG_EXECUTOR_MODULE", "")

if str(DAG_ROOT) not in sys.path:
    sys.path.insert(0, str(DAG_ROOT))

from common.mig_step_runtime import execute_logged_step


def metadata_connection() -> Any:
    connection = BaseHook.get_connection(META_CONN_ID)
    return psycopg.connect(host=connection.host, port=connection.port or 5439, dbname=connection.schema, user=connection.login, password=connection.password, connect_timeout=15, row_factory=dict_row)


def safe_name(value: object) -> str:
    name = str(value or "").strip()
    if not name:
        raise ValueError("파일 식별값이 없습니다.")
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)


def write_log(dag_id: str, record: dict[str, Any], step: str, status: str, message: str) -> None:
    started_at = record.get("wrk_stt_dtm") or datetime.now()
    finished_at = datetime.now() if status in {"SUCCESS", "FAILED"} else None
    elapsed_seconds = int((finished_at - started_at).total_seconds()) if finished_at else None
    directory = LOG_ROOT / safe_name(dag_id) / safe_name(record["dag_run_id"])
    directory.mkdir(parents=True, exist_ok=True)
    log_path = directory / f"{safe_name(record.get('mpg_id') or 'subject')}_{safe_name(step)}.log"
    payload = {"wrk_dtm": datetime.now().isoformat(timespec="seconds"), "dag_id": dag_id, "dag_run_id": record["dag_run_id"], "exec_run_id": record.get("exec_run_id"), "mpg_id": record.get("mpg_id"), "wrk_step_cd": step, "wrk_sts_cd": status, "wrk_msg": message}
    with log_path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(payload, ensure_ascii=False) + "\\n")
    query = """INSERT INTO mig_meta.tb_mig_run_log (wrk_dt, dag_nm, dag_run_id, task_nm, map_idx, exec_run_id, mpg_id, manf_id, meta_ver_no, s3_manf_path, load_mthd_cd, ins_scope_cd, sql_file_path, log_file_path, wrk_cnd_val, wrk_step_cd, wrk_sts_cd, wrk_msg, src_row_cnt, tgt_row_cnt, src_size_byte, tgt_size_byte, wrk_stt_dtm, wrk_end_dtm, wrk_elps_sec) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    with metadata_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (started_at.date(), dag_id, record["dag_run_id"], record.get("task_name"), record.get("map_index"), record.get("exec_run_id") or record["dag_run_id"], record.get("mpg_id"), record.get("manf_id"), record.get("meta_ver_no"), record.get("s3_manf_path"), record.get("load_mthd_cd"), record.get("ins_scope_cd"), record.get("sql_file_path"), str(log_path), record.get("wrk_cnd_val"), step, status, message, record.get("src_row_cnt"), record.get("tgt_row_cnt"), record.get("src_size_byte"), record.get("tgt_size_byte"), started_at, finished_at, elapsed_seconds))
        connection.commit()


def column_mappings(mapping_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    if not mapping_ids:
        return {}
    query = """SELECT mpg_id, col_ord, src_col_no, src_col_nm, src_data_type, src_null_yn, src_key_role_cd, tgt_col_no, tgt_col_nm, tgt_data_type, tgt_null_yn, tgt_key_role_cd, trnsf_expr, dflt_expr, sum_vald_yn, hsh_vald_yn FROM mig_meta.tb_mig_col_mpg WHERE active_yn = TRUE AND mpg_id = ANY(%s) ORDER BY mpg_id, col_ord"""
    with metadata_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (mapping_ids,))
            result: dict[int, list[dict[str, Any]]] = {}
            for row in cursor.fetchall():
                item = dict(row)
                result.setdefault(int(item["mpg_id"]), []).append(item)
    return result


def mapping_rows(subject_area_cd: str, dag_id: str, dag_run_id: str, conf: dict[str, Any], load_type: str) -> list[dict[str, Any]]:
    load_group = str(conf.get("LOAD_GROUP_CD") or "FULL").upper()
    if load_group not in {"FULL", "INCR"}:
        raise ValueError("LOAD_GROUP_CD는 FULL 또는 INCR이어야 합니다.")
    status_filter = "load_sts_cd = 'FULL'" if load_group == "FULL" else "load_sts_cd = 'INCR'"
    query = f"""SELECT mpg_id, prj_cd, sbj_area_cd, src_conn_id, src_sch_nm, src_tbl_nm, tgt_conn_id, tgt_sch_nm, tgt_tbl_nm, load_sts_cd AS load_mthd_cd, load_sts_cd, incr_basis_cd, incr_basis_col_nm, parl_mthd_cd, parl_cnd_arr, tgt_ddl_sql, meta_ver_no, s3_stg_path AS s3_base_path, s3_rlt_path, src_af_conn_id, s3_af_conn_id, tgt_af_conn_id FROM mig_meta.vw_mig_dag_tbl_mpg WHERE sbj_area_cd = %s AND {status_filter} ORDER BY tgt_sch_nm, tgt_tbl_nm, mpg_id"""
    parameters = (subject_area_cd,)
    with metadata_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
            rows = [dict(row) for row in cursor.fetchall()]
    required_connections = ("src_af_conn_id", "s3_af_conn_id") if load_type == "S3" else ("s3_af_conn_id", "tgt_af_conn_id")
    invalid = [str(row.get("mpg_id")) for row in rows if any(not row.get(column) for column in required_connections)]
    if invalid:
        raise ValueError(f"Airflow 연결명으로 사용할 접속ID가 없는 테이블매핑입니다: {', '.join(invalid)}")
    columns_by_mapping = column_mappings([int(row["mpg_id"]) for row in rows])
    missing_columns = [str(row["mpg_id"]) for row in rows if not columns_by_mapping.get(int(row["mpg_id"]))]
    if missing_columns:
        raise ValueError(f"활성 컬럼매핑이 없는 테이블매핑입니다: {', '.join(missing_columns)}")
    if load_type == "INS":
        run_id = str(conf.get("MIG_EXEC_ID") or "")
        if not run_id:
            raise ValueError("INS_ONLY 실행에는 MIG_EXEC_ID가 필요합니다.")
        approved = """SELECT M.manf_id, M.mpg_id, M.s3_manf_path FROM mig_meta.tb_mig_s3_manf M JOIN (SELECT mpg_id, MAX(manf_id) AS manf_id FROM mig_meta.tb_mig_s3_manf WHERE src_s3_vald_sts_cd = 'SUCCESS' AND tgt_aply_sts_cd = 'READY' GROUP BY mpg_id) L ON L.mpg_id = M.mpg_id AND L.manf_id = M.manf_id WHERE M.exec_run_id = %s"""
        with metadata_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(approved, (run_id,))
                manifests = {int(row["mpg_id"]): (row["manf_id"], row["s3_manf_path"]) for row in cursor.fetchall()}
        rows = [row for row in rows if int(row["mpg_id"]) in manifests]
        for row in rows:
            row["manf_id"], row["s3_manf_path"] = manifests[int(row["mpg_id"])]
    for index, row in enumerate(rows):
        row["dag_run_id"] = dag_run_id
        row["exec_run_id"] = str(conf.get("MIG_EXEC_ID") or dag_run_id)
        row["map_index"] = index
        row["wrk_cnd_val"] = json.dumps(conf, ensure_ascii=False, sort_keys=True)
        row["column_mappings"] = columns_by_mapping[int(row["mpg_id"])]
        row["ins_scope_cd"] = load_group
        row["sql_dir_nm"] = str(row["sbj_area_cd"]).lower()
        row["tbl_disp_nm"] = f"{row['tgt_sch_nm']}.{row['tgt_tbl_nm']}"
    return rows


def one_time_mapping_rows(table_configs: list[dict[str, Any]], dag_id: str, dag_run_id: str, conf: dict[str, Any], load_type: str) -> list[dict[str, Any]]:
    if load_type not in {"S3", "INS"}:
        raise ValueError("일회성 작업유형을 확인하십시오.")
    mapping_ids = [int(item["mpg_id"]) for item in table_configs]
    if not mapping_ids or len(mapping_ids) != len(set(mapping_ids)):
        raise ValueError("일회성 실행 테이블 목록을 확인하십시오.")
    settings = {int(item["mpg_id"]): item for item in table_configs}
    query = """SELECT mpg_id, prj_cd, sbj_area_cd, src_conn_id, src_sch_nm, src_tbl_nm, tgt_conn_id, tgt_sch_nm, tgt_tbl_nm, 'FULL' AS load_mthd_cd, load_sts_cd, incr_basis_cd, incr_basis_col_nm, tgt_ddl_sql, meta_ver_no, s3_stg_path AS s3_base_path, s3_rlt_path, src_af_conn_id, s3_af_conn_id, tgt_af_conn_id FROM mig_meta.vw_mig_dag_tbl_mpg WHERE mpg_id = ANY(%s) ORDER BY tgt_sch_nm, tgt_tbl_nm, mpg_id"""
    with metadata_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (mapping_ids,))
            rows = [dict(row) for row in cursor.fetchall()]
    if not rows:
        raise ValueError("일회성 실행 테이블매핑이 없습니다.")
    missing_mappings = sorted(set(mapping_ids) - {int(row["mpg_id"]) for row in rows})
    if missing_mappings:
        raise ValueError(f"일회성 실행 대상이 아닌 테이블매핑입니다: {', '.join(map(str, missing_mappings))}")
    for row in rows:
        setting = settings[int(row["mpg_id"])]
        row["parl_mthd_cd"] = setting["parl_mthd_cd"]
        row["parl_cnd_arr"] = setting.get("parl_cnd_arr")
    required_connections = ("src_af_conn_id", "s3_af_conn_id") if load_type == "S3" else ("s3_af_conn_id", "tgt_af_conn_id")
    invalid = [str(row.get("mpg_id")) for row in rows if any(not row.get(column) for column in required_connections)]
    if invalid:
        raise ValueError(f"Airflow 연결명으로 사용할 접속ID가 없는 일회성 테이블매핑입니다: {', '.join(invalid)}")
    columns_by_mapping = column_mappings([int(row["mpg_id"]) for row in rows])
    missing_columns = [str(row["mpg_id"]) for row in rows if not columns_by_mapping.get(int(row["mpg_id"]))]
    if missing_columns:
        raise ValueError(f"활성 컬럼매핑이 없는 일회성 테이블매핑입니다: {', '.join(missing_columns)}")
    if load_type == "INS":
        run_id = str(conf.get("MIG_EXEC_ID") or "")
        if not run_id:
            raise ValueError("일회성 INS 실행에는 MIG_EXEC_ID가 필요합니다.")
        approved = """SELECT M.manf_id, M.mpg_id, M.s3_manf_path FROM mig_meta.tb_mig_s3_manf M JOIN (SELECT mpg_id, MAX(manf_id) AS manf_id FROM mig_meta.tb_mig_s3_manf WHERE src_s3_vald_sts_cd = 'SUCCESS' AND tgt_aply_sts_cd = 'READY' GROUP BY mpg_id) L ON L.mpg_id = M.mpg_id AND L.manf_id = M.manf_id WHERE M.exec_run_id = %s"""
        with metadata_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(approved, (run_id,))
                manifests = {int(row["mpg_id"]): (row["manf_id"], row["s3_manf_path"]) for row in cursor.fetchall()}
        rows = [row for row in rows if int(row["mpg_id"]) in manifests]
        for row in rows:
            row["manf_id"], row["s3_manf_path"] = manifests[int(row["mpg_id"])]
    for index, row in enumerate(rows):
        row["dag_run_id"] = dag_run_id
        row["exec_run_id"] = str(conf.get("MIG_EXEC_ID") or dag_run_id)
        row["map_index"] = index
        row["wrk_cnd_val"] = json.dumps(conf, ensure_ascii=False, sort_keys=True)
        row["column_mappings"] = columns_by_mapping[int(row["mpg_id"])]
        row["ins_scope_cd"] = "ONCE"
        row["sql_dir_nm"] = str(row["sbj_area_cd"]).lower()
        row["tbl_disp_nm"] = f"{row['tgt_sch_nm']}.{row['tgt_tbl_nm']}"
    return rows


def s3_parallel_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    parts: list[dict[str, Any]] = []
    for row in rows:
        method = str(row.get("parl_mthd_cd") or "NONE").upper()
        raw_conditions = row.get("parl_cnd_arr") or "[]"
        try:
            conditions = json.loads(raw_conditions) if isinstance(raw_conditions, str) else list(raw_conditions)
        except (TypeError, ValueError) as error:
            raise ValueError(f"테이블매핑 {row['mpg_id']}의 S3추출병렬조건배열이 올바르지 않습니다.") from error
        if method == "NONE":
            conditions = [None]
        elif method != "WHERE" or not isinstance(conditions, list) or not conditions or any(not str(value).strip() for value in conditions):
            raise ValueError(f"테이블매핑 {row['mpg_id']}의 S3추출병렬조건배열을 확인하십시오.")
        for sequence, condition in enumerate(conditions, start=1):
            item = dict(row)
            item["src_where_cnd"] = None if condition is None else str(condition).strip()
            item["parl_seq_no"] = sequence
            item["parl_cnt"] = len(conditions)
            item["map_index"] = len(parts)
            if len(conditions) > 1:
                item["tbl_disp_nm"] = f"{item['tbl_disp_nm']} [{sequence}/{len(conditions)}]"
            condition_values = json.loads(item["wrk_cnd_val"])
            condition_values["SRC_WHERE_CND"] = item["src_where_cnd"]
            condition_values["PARL_SEQ_NO"] = sequence
            condition_values["PARL_CNT"] = len(conditions)
            item["wrk_cnd_val"] = json.dumps(condition_values, ensure_ascii=False, sort_keys=True)
            parts.append(item)
    return parts


def write_mapping_sql(record: dict[str, Any]) -> dict[str, Any]:
    directory = SQL_ROOT / safe_name(record["sql_dir_nm"])
    directory.mkdir(parents=True, exist_ok=True)
    sql_path = directory / f"{safe_name(record['tgt_tbl_nm'])}.sql"
    sql_text = str(record.get("tgt_ddl_sql") or "").strip()
    if sql_text:
        sql_path.write_text(sql_text + "\\n", encoding="utf-8")
        record["sql_file_path"] = str(sql_path)
    return record
'''


def worker_source(sbj_area_cd: str, up_sbj_area_cd: str, dag_dvsn_cd: str, default_parallel: int, maximum_parallel: int) -> str:
    code = text(sbj_area_cd).upper()
    parent = text(up_sbj_area_cd).upper()
    division = text(dag_dvsn_cd).upper()
    name = dag_id(code, division)
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,7}", parent):
        raise ValueError("상위주제영역코드를 확인하십시오.")
    if default_parallel < 1 or maximum_parallel < default_parallel:
        raise ValueError("기본·최대 병렬 값을 확인하십시오.")
    step = "S3" if division == "S3" else "INS"
    label = "원천 S3 적재" if division == "S3" else "대상 적재"
    execution_line = "execute.expand(record=expand_s3(load_mappings()))" if division == "S3" else "execute.expand(record=write_sql.expand(record=load_mappings()))"
    return COMMON_SOURCE + f'''
import pendulum
from airflow.decorators import dag, task

DAG_ID = "{name}"
SBJ_AREA_CD = "{code}"
DFLT_PARL_CNT = {default_parallel}
MAX_PARL_CNT = {maximum_parallel}

@dag(dag_id=DAG_ID, start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"), schedule=None, catchup=False, max_active_tasks=MAX_PARL_CNT, tags=["mig", "{parent.lower()}", "{code.lower()}", "{division.lower()}"])
def migration_worker() -> None:
    @task
    def load_mappings(**context: Any) -> list[dict[str, Any]]:
        return mapping_rows(SBJ_AREA_CD, DAG_ID, context["dag_run"].run_id, context["dag_run"].conf or {{}}, "{step}")

    @task
    def expand_s3(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return s3_parallel_rows(records)

    @task(map_index_template="{{{{ task.parameters['record']['tbl_disp_nm'] }}}}")
    def write_sql(record: dict[str, Any]) -> dict[str, Any]:
        return write_mapping_sql(record)

    @task(max_active_tis_per_dag=DFLT_PARL_CNT, map_index_template="{{{{ task.parameters['record']['tbl_disp_nm'] }}}}")
    def execute(record: dict[str, Any]) -> dict[str, Any]:
        return execute_logged_step(record, "{step}", "{label}", EXECUTOR_MODULE, lambda active, work_step, status, message: write_log(DAG_ID, active, work_step, status, message))

    {execution_line}

migration_worker()
'''


def schedule_expression(schedule_code: object) -> str | None:
    return {"NONE": None, "DLY_0100": "0 1 * * *", "DLY_0200": "0 2 * * *", "DLY_0300": "0 3 * * *", "DLY_0400": "0 4 * * *", "DLY_0500": "0 5 * * *"}.get(text(schedule_code).upper())


def controller_source(sbj_area_cd: str, up_sbj_area_cd: str, controller_type: str, incr_schedule: object = "NONE") -> str:
    code = text(sbj_area_cd).upper()
    parent = text(up_sbj_area_cd).upper()
    division = text(controller_type).upper()
    if division not in {"FULL_CTL", "INCR_CTL"}:
        raise ValueError("제어 DAG구분코드를 확인하십시오.")
    name = dag_id(code, division)
    s3_name = dag_id(code, "S3")
    ins_name = dag_id(code, "INS")
    load_group = "FULL" if division == "FULL_CTL" else "INCR"
    default_mode = "S3_INS" if division == "FULL_CTL" else "S3_ONLY"
    schedule = None if division == "FULL_CTL" else schedule_expression(incr_schedule)
    schedule_text = "None" if schedule is None else repr(schedule)
    return COMMON_SOURCE + f'''
import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "{name}"
SBJ_AREA_CD = "{code}"

@dag(dag_id=DAG_ID, start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"), schedule={schedule_text}, catchup=False, tags=["mig", "{parent.lower()}", "{code.lower()}", "{load_group.lower()}", "ctl"])
def migration_controller() -> None:
    @task
    def prepare_execution(**context: Any) -> dict[str, Any]:
        payload = dict(context["dag_run"].conf or {{}})
        payload.setdefault("MIG_EXEC_ID", context["dag_run"].run_id)
        payload.setdefault("LOAD_GROUP_CD", "{load_group}")
        payload.setdefault("EXEC_MODE", "{default_mode}")
        if str(payload["LOAD_GROUP_CD"]).upper() != "{load_group}":
            raise ValueError("이 DAG의 적재구분과 LOAD_GROUP_CD가 다릅니다.")
        return payload

    @task.branch
    def select_execution(payload: dict[str, Any]) -> str:
        mode = str(payload.get("EXEC_MODE", "S3_INS")).upper()
        routes = {{"S3_ONLY": "run_s3", "S3_INS": "run_s3", "INS_ONLY": "run_ins_only"}}
        if mode not in routes:
            raise ValueError("EXEC_MODE는 S3_ONLY, S3_INS, INS_ONLY 중 하나여야 합니다.")
        return routes[mode]

    @task.branch
    def select_after_src_s3(payload: dict[str, Any]) -> str:
        return "run_ins_after_s3" if str(payload.get("EXEC_MODE", "S3_INS")).upper() == "S3_INS" else "s3_only_completed"

    @task
    def validate_src_s3(payload: dict[str, Any], **context: Any) -> None:
        record = {{"dag_run_id": context["dag_run"].run_id, "exec_run_id": str(payload["MIG_EXEC_ID"]), "wrk_cnd_val": json.dumps(payload, ensure_ascii=False, sort_keys=True), "task_name": "validate_src_s3"}}
        execute_logged_step(record, "VALIDATE_SRC_S3", "원천 S3 검증", EXECUTOR_MODULE, lambda active, step, status, message: write_log(DAG_ID, active, step, status, message))

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def validate_s3_tgt(payload: dict[str, Any], **context: Any) -> None:
        record = {{"dag_run_id": context["dag_run"].run_id, "exec_run_id": str(payload["MIG_EXEC_ID"]), "wrk_cnd_val": json.dumps(payload, ensure_ascii=False, sort_keys=True), "task_name": "validate_s3_tgt"}}
        execute_logged_step(record, "VALIDATE_S3_TGT", "S3 대상 검증", EXECUTOR_MODULE, lambda active, step, status, message: write_log(DAG_ID, active, step, status, message))

    payload = prepare_execution()
    route = select_execution(payload)
    run_s3 = TriggerDagRunOperator(task_id="run_s3", trigger_dag_id="{s3_name}", conf="{{{{ ti.xcom_pull(task_ids='prepare_execution') | tojson }}}}", wait_for_completion=True)
    run_ins_after_s3 = TriggerDagRunOperator(task_id="run_ins_after_s3", trigger_dag_id="{ins_name}", conf="{{{{ ti.xcom_pull(task_ids='prepare_execution') | tojson }}}}", wait_for_completion=True)
    run_ins_only = TriggerDagRunOperator(task_id="run_ins_only", trigger_dag_id="{ins_name}", conf="{{{{ ti.xcom_pull(task_ids='prepare_execution') | tojson }}}}", wait_for_completion=True)
    source_validation = validate_src_s3(payload)
    post_source_route = select_after_src_s3(payload)
    target_validation = validate_s3_tgt(payload)
    s3_only_completed = EmptyOperator(task_id="s3_only_completed")
    completed = EmptyOperator(task_id="completed", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    route >> run_s3 >> source_validation >> post_source_route >> run_ins_after_s3 >> target_validation >> completed
    route >> run_ins_only >> target_validation
    post_source_route >> s3_only_completed >> completed

migration_controller()
'''


def once_dag_id(once_wrk_id: str) -> str:
    token = text(once_wrk_id).upper()
    if not re.fullmatch(r"ONCE_[A-Z0-9_]{8,35}", token):
        raise ValueError("일회성작업ID를 확인하십시오.")
    return f"mig_{token.lower()}_ctl"


def once_controller_source(once_wrk_id: str, run_mode: str, table_configs: list[dict[str, Any]], reason: str) -> str:
    work_id = text(once_wrk_id).upper()
    mode = text(run_mode).upper()
    if mode not in {"S3_ONLY", "S3_INS"}:
        raise ValueError("일회성 실행방식은 S3_ONLY 또는 S3_INS여야 합니다.")
    if not text(reason):
        raise ValueError("일회성 실행 사유를 입력하십시오.")
    normalized_tables: list[dict[str, Any]] = []
    for config in table_configs:
        mapping_id_value = config.get("mpg_id")
        if not isinstance(mapping_id_value, int) or mapping_id_value < 1:
            raise ValueError("일회성 실행 테이블매핑ID를 확인하십시오.")
        method = text(config.get("parl_mthd_cd")).upper()
        conditions = config.get("parl_cnd_arr")
        if method not in {"NONE", "WHERE"}:
            raise ValueError("일회성 S3추출방식은 NONE 또는 WHERE여야 합니다.")
        if method == "NONE":
            conditions = None
        elif not isinstance(conditions, list) or not conditions or any(not text(value) for value in conditions):
            raise ValueError("일회성 WHERE 병렬조건배열을 확인하십시오.")
        normalized_tables.append({"mpg_id": mapping_id_value, "parl_mthd_cd": method, "parl_cnd_arr": conditions})
    if not normalized_tables or len({item["mpg_id"] for item in normalized_tables}) != len(normalized_tables):
        raise ValueError("일회성 실행 테이블 목록을 확인하십시오.")
    name = once_dag_id(work_id)
    table_json = json.dumps(normalized_tables, ensure_ascii=False, sort_keys=True)
    reason_json = json.dumps(text(reason), ensure_ascii=False)
    if mode == "S3_INS":
        branch = '''
    ins_records = load_ins(payload)
    ins_execute = execute_ins.expand(record=write_sql.expand(record=ins_records))
    target_validation = validate_s3_tgt(payload)
    source_validation >> ins_records
    ins_execute >> target_validation >> finalize(payload)
'''
        final_dependency = "s3_execute >> source_validation"
    else:
        branch = '''
    s3_execute >> source_validation >> finalize(payload)
'''
        final_dependency = ""
    return COMMON_SOURCE + f'''
import pendulum
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "{name}"
RUN_MODE_CD = "{mode}"
ONE_TIME_TABLES = json.loads({table_json!r})
ONE_TIME_REASON = json.loads({reason_json!r})

@dag(dag_id=DAG_ID, start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"), schedule=None, catchup=False, tags=["mig", "once", "{mode.lower()}"])
def once_migration() -> None:
    @task
    def prepare_execution(**context: Any) -> dict[str, Any]:
        payload = dict(context["dag_run"].conf or {{}})
        payload.setdefault("MIG_EXEC_ID", context["dag_run"].run_id)
        payload["RUN_MODE_CD"] = RUN_MODE_CD
        payload["ONE_TIME_YN"] = "Y"
        payload["ONE_TIME_RSN"] = ONE_TIME_REASON
        payload["ONE_TIME_TBL"] = ONE_TIME_TABLES
        return payload

    @task
    def load_s3(payload: dict[str, Any], **context: Any) -> list[dict[str, Any]]:
        return one_time_mapping_rows(ONE_TIME_TABLES, DAG_ID, context["dag_run"].run_id, payload, "S3")

    @task
    def load_ins(payload: dict[str, Any], **context: Any) -> list[dict[str, Any]]:
        return one_time_mapping_rows(ONE_TIME_TABLES, DAG_ID, context["dag_run"].run_id, payload, "INS")

    @task
    def expand_s3(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return s3_parallel_rows(records)

    @task(max_active_tis_per_dag=8, map_index_template="{{{{ task.parameters['record']['tbl_disp_nm'] }}}}")
    def execute_s3(record: dict[str, Any]) -> dict[str, Any]:
        return execute_logged_step(record, "S3", "일회성 원천 S3 적재", EXECUTOR_MODULE, lambda active, step, status, message: write_log(DAG_ID, active, step, status, message))

    @task(map_index_template="{{{{ task.parameters['record']['tbl_disp_nm'] }}}}")
    def write_sql(record: dict[str, Any]) -> dict[str, Any]:
        return write_mapping_sql(record)

    @task(max_active_tis_per_dag=1, map_index_template="{{{{ task.parameters['record']['tbl_disp_nm'] }}}}")
    def execute_ins(record: dict[str, Any]) -> dict[str, Any]:
        return execute_logged_step(record, "INS", "일회성 대상 적재", EXECUTOR_MODULE, lambda active, step, status, message: write_log(DAG_ID, active, step, status, message))

    @task
    def validate_src_s3(payload: dict[str, Any], **context: Any) -> None:
        record = {{"dag_run_id": context["dag_run"].run_id, "exec_run_id": str(payload["MIG_EXEC_ID"]), "ins_scope_cd": "ONCE", "wrk_cnd_val": json.dumps(payload, ensure_ascii=False, sort_keys=True), "task_name": "validate_src_s3"}}
        execute_logged_step(record, "VALIDATE_SRC_S3", "일회성 원천 S3 검증", EXECUTOR_MODULE, lambda active, step, status, message: write_log(DAG_ID, active, step, status, message))

    @task
    def validate_s3_tgt(payload: dict[str, Any], **context: Any) -> None:
        record = {{"dag_run_id": context["dag_run"].run_id, "exec_run_id": str(payload["MIG_EXEC_ID"]), "ins_scope_cd": "ONCE", "wrk_cnd_val": json.dumps(payload, ensure_ascii=False, sort_keys=True), "task_name": "validate_s3_tgt"}}
        execute_logged_step(record, "VALIDATE_S3_TGT", "일회성 S3 대상 검증", EXECUTOR_MODULE, lambda active, step, status, message: write_log(DAG_ID, active, step, status, message))

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def finalize(payload: dict[str, Any], **context: Any) -> None:
        with metadata_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) AS failed_count FROM mig_meta.tb_mig_run_log WHERE dag_nm = %s AND exec_run_id = %s AND wrk_sts_cd = 'FAILED'", (DAG_ID, str(payload["MIG_EXEC_ID"])))
                failed_count = int(cursor.fetchone()["failed_count"])
        failed_tasks = [instance.task_id for instance in context["dag_run"].get_task_instances() if str(instance.state).upper().endswith("FAILED")]
        failed = bool(failed_count or failed_tasks)
        message = "실패 작업: " + ", ".join(failed_tasks) if failed_tasks else ("실패 실행 로그가 있습니다." if failed_count else "정상 완료")
        record = {{"dag_run_id": context["dag_run"].run_id, "exec_run_id": str(payload["MIG_EXEC_ID"]), "ins_scope_cd": "ONCE", "wrk_cnd_val": json.dumps(payload, ensure_ascii=False, sort_keys=True), "task_name": "finalize"}}
        write_log(DAG_ID, record, "ONCE_SUMMARY", "FAILED" if failed else "SUCCESS", message)
        if failed:
            raise RuntimeError(message)

    payload = prepare_execution()
    s3_records = load_s3(payload)
    s3_execute = execute_s3.expand(record=expand_s3(s3_records))
    source_validation = validate_src_s3(payload)
{branch}
    {final_dependency}

once_migration()
'''


def project_source(project_code: str, subject_rows: list[dict[str, Any]], phase: str = "FULL") -> str:
    project = text(project_code).upper()
    normalized_phase = text(phase).upper()
    if normalized_phase not in {"FULL", "INCR"}:
        raise ValueError("프로젝트 실행구분은 FULL 또는 INCR이어야 합니다.")
    name = project_dag_id(project, normalized_phase)
    tasks: dict[str, str] = {}
    dependencies: dict[str, list[str]] = {}
    for row in subject_rows:
        code = text(row["sbj_area_cd"]).upper()
        tasks[code] = dag_id(code, f"{normalized_phase}_CTL")
        dependencies[code] = [text(value).upper() for value in text(row.get("pre_sbj_area_cds")).split(",") if text(value)]
    if not tasks:
        raise ValueError("프로젝트에 사용 중인 실행 주제영역이 없습니다.")
    missing = sorted({pre for values in dependencies.values() for pre in values if pre not in tasks})
    if missing:
        raise ValueError("프로젝트에 포함되지 않은 선행 주제영역이 있습니다: " + ", ".join(missing))
    roots = [code for code in sorted(tasks) if not dependencies[code]]
    if not roots:
        raise ValueError("주제영역 선후행이 순환합니다.")
    task_names = {code: f"run_{code.lower()}" for code in tasks}
    task_lines = [f'    {task_names[code]} = TriggerDagRunOperator(task_id="{task_names[code]}", trigger_dag_id="{tasks[code]}", conf="{{{{ ti.xcom_pull(task_ids=\'prepare_execution\') | tojson }}}}", wait_for_completion=True, trigger_rule=TriggerRule.ALL_DONE)' for code in sorted(tasks)]
    dependency_lines = [f"    {task_names[pre_code]} >> {task_names[code]}" for code in sorted(tasks) for pre_code in dependencies[code]]
    final_lines = [f"    {task_names[code]} >> summary" for code in sorted(tasks) if not any(code in values for values in dependencies.values())]
    return COMMON_SOURCE + f'''
import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "{name}"

@dag(dag_id=DAG_ID, start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"), schedule=None, catchup=False, tags=["mig", "{project.lower()}", "orch"])
def migration_orchestrator() -> None:
    @task
    def prepare_execution(**context: Any) -> dict[str, Any]:
        payload = dict(context["dag_run"].conf or {{}})
        payload.setdefault("MIG_EXEC_ID", context["dag_run"].run_id)
        payload.setdefault("LOAD_GROUP_CD", "{normalized_phase}")
        payload.setdefault("EXEC_MODE", "S3_INS" if "{normalized_phase}" == "FULL" else "S3_ONLY")
        if str(payload["LOAD_GROUP_CD"]).upper() != "{normalized_phase}":
            raise ValueError("이 DAG의 적재구분과 LOAD_GROUP_CD가 다릅니다.")
        return payload
    payload = prepare_execution()
{chr(10).join(task_lines)}
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def final_summary(payload: dict[str, Any], **context: Any) -> None:
        record = {{"dag_run_id": context["dag_run"].run_id, "exec_run_id": str(payload["MIG_EXEC_ID"]), "wrk_cnd_val": json.dumps(payload, ensure_ascii=False, sort_keys=True), "task_name": "final_summary"}}
        execute_logged_step(record, "ORCH_SUMMARY", "프로젝트 실행 집계", EXECUTOR_MODULE, lambda active, step, status, message: write_log(DAG_ID, active, step, status, message))
    summary = final_summary(payload)
    payload >> [{', '.join(task_names[code] for code in roots)}]
{chr(10).join(dependency_lines)}
{chr(10).join(final_lines)}

migration_orchestrator()
'''


def dag_source(sbj_area_cd: str, up_sbj_area_cd: str, dag_name: str, default_parallel: int, maximum_parallel: int) -> str:
    name = text(dag_name).lower()
    if name.endswith("_s3"):
        return worker_source(sbj_area_cd, up_sbj_area_cd, "S3", default_parallel, maximum_parallel)
    if name.endswith("_ins"):
        return worker_source(sbj_area_cd, up_sbj_area_cd, "INS", default_parallel, maximum_parallel)
    if name.endswith("_full_ctl"):
        return controller_source(sbj_area_cd, up_sbj_area_cd, "FULL_CTL")
    if name.endswith("_incr_ctl"):
        return controller_source(sbj_area_cd, up_sbj_area_cd, "INCR_CTL")
    raise ValueError("DAG ID는 _s3, _ins, _full_ctl, _incr_ctl로 끝나야 합니다.")


def save_dag_files(sbj_area_cd: str, sources: dict[str, str]) -> list[Path]:
    DAG_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for name, source in sources.items():
        path = DAG_OUTPUT_ROOT / f"{name}.py"
        path.write_text(source, encoding="utf-8")
        paths.append(path)
    return paths


def save_dag_settings(values: dict[str, Any], schema_name: str, connect: Any, qualified: Any, sbj_area_cd: str, settings: dict[str, int | str]) -> None:
    if any(int(settings[key]) < 1 for key in ("s3_default", "s3_maximum", "ins_default", "ins_maximum")):
        raise ValueError("DAG 병렬도는 1 이상이어야 합니다.")
    if int(settings["s3_default"]) > int(settings["s3_maximum"]) or int(settings["ins_default"]) > int(settings["ins_maximum"]):
        raise ValueError("DAG 최대 병렬은 기본 병렬 이상이어야 합니다.")
    schedules = {"NONE", "DLY_0100", "DLY_0200", "DLY_0300", "DLY_0400", "DLY_0500"}
    if text(settings["incr_schedule"]).upper() not in schedules:
        raise ValueError("증분 일정코드를 확인하십시오.")
    table_name = qualified(schema_name, "tb_mig_sbj_dag_mpg")
    rows = (("S3", settings["s3_default"], settings["s3_maximum"], "NONE"), ("INS", settings["ins_default"], settings["ins_maximum"], "NONE"), ("INCR_CTL", 1, 1, settings["incr_schedule"]))
    with connect(values) as connection:
        with connection.cursor() as cursor:
            for division, default_parallel, maximum_parallel, schedule in rows:
                cursor.execute(f"UPDATE {table_name} SET dflt_parl_cnt = %s, max_parl_cnt = %s, schd_cd = %s, upd_dtm = GETDATE() WHERE sbj_area_cd = %s AND dag_dvsn_cd = %s", (int(default_parallel), int(maximum_parallel), text(schedule).upper(), sbj_area_cd, division))
                if cursor.rowcount != 1:
                    raise ValueError(f"DAG 설정을 찾을 수 없습니다: {sbj_area_cd} / {division}")
        connection.commit()


def render_dag_generator(areas: pd.DataFrame, values: dict[str, Any], schema_name: str, connect: Any, qualified: Any, enabled: bool) -> None:
    candidates = areas.loc[areas.up_sbj_area_cd.map(text).ne("") & areas.active_yn.fillna(False).astype(bool)].copy()
    if candidates.empty:
        st.info("사용 중인 실행 주제영역이 없습니다.", icon=":material/info:")
        return
    selected_code = st.selectbox("실행 주제영역", candidates.sbj_area_cd.tolist(), key="dag_generator_area")
    current = candidates.loc[candidates.sbj_area_cd.eq(selected_code)].iloc[0]
    s3_default = int(current.get("s3_dflt_parl_cnt", 1) or 1)
    s3_maximum = int(current.get("s3_max_parl_cnt", 1) or 1)
    ins_default = int(current.get("ins_dflt_parl_cnt", 1) or 1)
    ins_maximum = int(current.get("ins_max_parl_cnt", 1) or 1)
    schedule_options = ["NONE", "DLY_0100", "DLY_0200", "DLY_0300", "DLY_0400", "DLY_0500"]
    current_schedule = text(current.get("incr_schd_cd", "NONE")).upper() or "NONE"
    with st.form("dag_settings_form"):
        st.markdown("##### DAG 설정")
        left, right = st.columns(2)
        with left:
            configured_s3_default = st.number_input("S3 기본 병렬", min_value=1, step=1, value=s3_default)
            configured_s3_maximum = st.number_input("S3 최대 병렬", min_value=1, step=1, value=s3_maximum)
        with right:
            configured_ins_default = st.number_input("INS 기본 병렬", min_value=1, step=1, value=ins_default)
            configured_ins_maximum = st.number_input("INS 최대 병렬", min_value=1, step=1, value=ins_maximum)
        configured_schedule = st.selectbox("증분 S3 일정", schedule_options, index=schedule_options.index(current_schedule) if current_schedule in schedule_options else 0)
        settings_saved = st.form_submit_button("DAG 설정 저장", type="primary")
    if settings_saved:
        try:
            save_dag_settings(values, schema_name, connect, qualified, selected_code, {"s3_default": configured_s3_default, "s3_maximum": configured_s3_maximum, "ins_default": configured_ins_default, "ins_maximum": configured_ins_maximum, "incr_schedule": configured_schedule})
            st.rerun()
        except Exception as error:
            st.error(str(error), icon=":material/error:")
            return
    try:
        sources = {
            dag_id(selected_code, "S3"): worker_source(selected_code, text(current.up_sbj_area_cd), "S3", s3_default, s3_maximum),
            dag_id(selected_code, "INS"): worker_source(selected_code, text(current.up_sbj_area_cd), "INS", ins_default, ins_maximum),
            dag_id(selected_code, "FULL_CTL"): controller_source(selected_code, text(current.up_sbj_area_cd), "FULL_CTL"),
            dag_id(selected_code, "INCR_CTL"): controller_source(selected_code, text(current.up_sbj_area_cd), "INCR_CTL", current.get("incr_schd_cd", "NONE")),
        }
        for name, source in sources.items():
            compile(source, f"{name}.py", "exec")
    except ValueError as error:
        st.error(str(error), icon=":material/error:")
        return
    st.success("S3·INS·전체 제어·증분 제어 DAG Python 문법 검증 완료", icon=":material/check_circle:")
    preview = st.selectbox("미리보기 DAG", list(sources))
    st.code(sources[preview], language="python")
    if st.button("주제영역 DAG 4종 생성", icon=":material/terminal:", type="primary", disabled=not enabled):
        paths = save_dag_files(text(selected_code), sources)
        st.success("생성 완료: " + ", ".join(path.name for path in paths))
    st.download_button("선택 DAG 다운로드", data=sources[preview], file_name=f"{preview}.py", mime="text/x-python", icon=":material/download:")


def render_project_dag_generator(areas: pd.DataFrame, maps: pd.DataFrame, enabled: bool) -> None:
    projects = sorted({text(value).upper() for value in maps.get("prj_cd", pd.Series(dtype=str)).tolist() if text(value)})
    if not projects:
        st.info("프로젝트코드가 있는 테이블매핑을 먼저 등록하십시오.", icon=":material/info:")
        return
    project = st.selectbox("프로젝트", projects, key="project_dag_generator")
    subject_codes = {text(value).upper() for value in maps.loc[maps.prj_cd.map(text).str.upper().eq(project), "sbj_area_cd"].tolist()}
    subjects = areas.loc[areas.sbj_area_cd.map(text).str.upper().isin(subject_codes) & areas.up_sbj_area_cd.map(text).ne("") & areas.active_yn.fillna(False).astype(bool)].copy()
    phase = st.selectbox("프로젝트 실행구분", ["FULL", "INCR"], format_func=lambda value: "초기·전체 실행" if value == "FULL" else "일 증분 S3 실행")
    try:
        rows = subjects[["sbj_area_cd", "pre_sbj_area_cds"]].to_dict(orient="records")
        source = project_source(project, rows, phase)
        compile(source, f"{project_dag_id(project, phase)}.py", "exec")
    except ValueError as error:
        st.error(str(error), icon=":material/error:")
        return
    st.success("프로젝트 오케스트레이터 Python 문법 검증 완료", icon=":material/check_circle:")
    st.code(source, language="python")
    name = project_dag_id(project, phase)
    if st.button("프로젝트 오케스트레이터 생성", icon=":material/terminal:", type="primary", disabled=not enabled):
        paths = save_dag_files(project, {name: source})
        st.success(f"생성 완료: {paths[0].name}")
    st.download_button("프로젝트 오케스트레이터 다운로드", data=source, file_name=f"{name}.py", mime="text/x-python", icon=":material/download:")

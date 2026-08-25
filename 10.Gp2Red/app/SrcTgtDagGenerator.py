from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st


DAG_OUTPUT_ROOT = Path(__file__).parent.parent / "dag"

DAG_TEMPLATE = '''from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pendulum
import psycopg
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from psycopg.rows import dict_row


DAG_ID = "__DAG_ID__"
SBJ_AREA_CD = "__SBJ_AREA_CD__"
UP_SBJ_AREA_CD = "__UP_SBJ_AREA_CD__"
DFLT_PARL_CNT = __DFLT_PARL_CNT__
MAX_PARL_CNT = __MAX_PARL_CNT__
META_CONN_ID = os.getenv("MIG_META_CONN_ID", "TGT_RED")
DAG_ROOT = Path(__file__).parent
SQL_ROOT = Path(os.getenv("MIG_SQL_ROOT", str(DAG_ROOT.parent / "sql")))
LOG_ROOT = Path(os.getenv("MIG_LOG_ROOT", str(DAG_ROOT.parent / "log")))
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


def sql_dir_name(value: object) -> str:
    name = str(value or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", name):
        raise ValueError(f"SQL 하위 폴더명이 올바르지 않습니다: {value}")
    return name


def target_table_name(value: object) -> str:
    name = str(value or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_$-]+", name):
        raise ValueError(f"대상 테이블명이 파일명으로 사용할 수 없습니다: {value}")
    return name


def write_log(record: dict[str, Any], step: str, status: str, message: str, sql_path: str | None = None) -> None:
    started_at = record.get("wrk_stt_dtm") or datetime.now()
    finished_at = datetime.now() if status in {"SUCCESS", "FAILED", "BLOCKED"} else None
    elapsed_seconds = int((finished_at - started_at).total_seconds()) if finished_at else None
    directory = LOG_ROOT / safe_name(DAG_ID) / safe_name(record["dag_run_id"])
    directory.mkdir(parents=True, exist_ok=True)
    log_path = directory / f"{safe_name(record.get('mpg_id'))}_{safe_name(step)}.log"
    payload = {"wrk_dtm": datetime.now().isoformat(timespec="seconds"), "dag_id": DAG_ID, "dag_run_id": record["dag_run_id"], "task_name": record.get("task_name"), "map_index": record.get("map_index"), "mpg_id": record.get("mpg_id"), "meta_ver_no": record.get("meta_ver_no"), "sql_file_path": sql_path, "wrk_step_cd": step, "wrk_sts_cd": status, "wrk_msg": message, "wrk_stt_dtm": started_at.isoformat(timespec="seconds"), "wrk_end_dtm": finished_at.isoformat(timespec="seconds") if finished_at else None, "wrk_elps_sec": elapsed_seconds}
    with log_path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(payload, ensure_ascii=False) + "\\n")
    query = """
        INSERT INTO mig_meta.tb_mig_run_log (
            wrk_dt, dag_nm, dag_run_id, task_nm, map_idx, mpg_id, meta_ver_no,
            exec_run_id, sql_file_path, log_file_path, wrk_cnd_val, wrk_step_cd, wrk_sts_cd, wrk_msg,
            src_row_cnt, tgt_row_cnt, src_size_byte, tgt_size_byte, wrk_stt_dtm, wrk_end_dtm, wrk_elps_sec
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with metadata_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (started_at.date(), DAG_ID, record["dag_run_id"], record.get("task_name"), record.get("map_index"), record.get("mpg_id"), record.get("meta_ver_no"), record.get("exec_run_id") or record["dag_run_id"], sql_path, str(log_path), record.get("wrk_cnd_val"), step, status, message, record.get("src_row_cnt"), record.get("tgt_row_cnt"), record.get("src_size_byte"), record.get("tgt_size_byte"), started_at, finished_at, elapsed_seconds))
        connection.commit()


@dag(dag_id=DAG_ID, start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"), schedule=None, catchup=False, max_active_tasks=MAX_PARL_CNT, tags=["mig", UP_SBJ_AREA_CD.lower(), SBJ_AREA_CD.lower()])
def subject_dag() -> None:
    @task
    def load_mappings(**context: Any) -> list[dict[str, Any]]:
        query = """
            SELECT M.mpg_id, M.sql_dir_nm, M.src_conn_id, SRC.conn_nm AS src_conn_nm, SRC.dbms_cd AS src_dbms_cd, SRC.sec_sect_nm AS src_sec_sect_nm, SRC.af_conn_id AS src_af_conn_id, M.src_sch_nm, M.src_tbl_nm,
                   M.tgt_conn_id, TGT.conn_nm AS tgt_conn_nm, TGT.dbms_cd AS tgt_dbms_cd, TGT.sec_sect_nm AS tgt_sec_sect_nm, TGT.af_conn_id AS tgt_af_conn_id, M.tgt_sch_nm, M.tgt_tbl_nm, M.load_mthd_cd, M.wm_col_nm, M.incr_where_tmpl,
                   M.trnsf_pfl_cd, M.s3_stg_path, M.s3_file_fmt_cd, M.tgt_ddl_sql, M.meta_ver_no,
                   M.dflt_parl_cnt, M.dag_max_parl_cnt
              FROM mig_meta.vw_mig_dag_tbl_mpg M
              LEFT JOIN mig_meta.tb_mig_conn SRC
                ON SRC.conn_id = M.src_conn_id
               AND SRC.conn_dvsn_cd = 'SRC'
               AND SRC.active_yn = TRUE
              LEFT JOIN mig_meta.tb_mig_conn TGT
                ON TGT.conn_id = M.tgt_conn_id
               AND TGT.conn_dvsn_cd = 'TGT'
               AND TGT.active_yn = TRUE
             WHERE M.sbj_area_cd = %s
               AND M.dag_id = %s
             ORDER BY M.tgt_sch_nm, M.tgt_tbl_nm, M.mpg_id
        """
        with metadata_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (SBJ_AREA_CD, DAG_ID))
                rows = [dict(row) for row in cursor.fetchall()]
        invalid = [str(row.get("mpg_id")) for row in rows if not str(row.get("src_af_conn_id") or "").strip() or not str(row.get("tgt_af_conn_id") or "").strip()]
        if invalid:
            raise ValueError(f"접속관리가 완료되지 않은 테이블매핑입니다: {', '.join(invalid)}")
        wrk_cnd_val = json.dumps(context["dag_run"].conf or {}, ensure_ascii=False, sort_keys=True)
        for index, row in enumerate(rows):
            row["dag_run_id"] = context["dag_run"].run_id
            row["map_index"] = index
            row["wrk_cnd_val"] = wrk_cnd_val
            row["tbl_disp_nm"] = f"{row['tgt_sch_nm']}.{row['tgt_tbl_nm']}"
        return rows

    @task(map_index_template="{{ task.parameters['record']['tbl_disp_nm'] }}")
    def write_sql(record: dict[str, Any]) -> dict[str, Any]:
        directory = SQL_ROOT / sql_dir_name(record["sql_dir_nm"])
        directory.mkdir(parents=True, exist_ok=True)
        sql_path = directory / f"{target_table_name(record['tgt_tbl_nm'])}.sql"
        ddl = str(record.get("tgt_ddl_sql") or "").strip()
        if ddl:
            sql_path.write_text(ddl + "\\n", encoding="utf-8")
            record["sql_file_path"] = str(sql_path)
        return record

    @task(max_active_tis_per_dag=DFLT_PARL_CNT, map_index_template="{{ task.parameters['record']['tbl_disp_nm'] }}")
    def extract(record: dict[str, Any]) -> dict[str, Any]:
        return execute_logged_step(record, "EXTRACT", "원천 추출", EXECUTOR_MODULE, lambda active_record, step, status, message: write_log(active_record, step, status, message, active_record.get("sql_file_path")))

    @task(max_active_tis_per_dag=DFLT_PARL_CNT, map_index_template="{{ task.parameters['record']['tbl_disp_nm'] }}")
    def load(record: dict[str, Any]) -> dict[str, Any]:
        return execute_logged_step(record, "LOAD", "대상 적재", EXECUTOR_MODULE, lambda active_record, step, status, message: write_log(active_record, step, status, message, active_record.get("sql_file_path")))

    load.expand(record=extract.expand(record=write_sql.expand(record=load_mappings())))


subject_dag()
'''


def text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def dag_source(sbj_area_cd: str, up_sbj_area_cd: str, dag_id: str, default_parallel: int, maximum_parallel: int) -> str:
    code = text(sbj_area_cd).upper()
    parent_code = text(up_sbj_area_cd).upper()
    name = text(dag_id).lower()
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,7}", code):
        raise ValueError("실행 주제영역 코드를 확인하십시오.")
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,7}", parent_code):
        raise ValueError("상위 주제영역 코드를 확인하십시오.")
    if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
        raise ValueError("DAG ID를 확인하십시오.")
    if default_parallel < 1 or maximum_parallel < default_parallel:
        raise ValueError("기본·최대 병렬 값을 확인하십시오.")
    source = DAG_TEMPLATE.replace("__DAG_ID__", name)
    source = source.replace("__SBJ_AREA_CD__", code)
    source = source.replace("__UP_SBJ_AREA_CD__", parent_code)
    source = source.replace("__DFLT_PARL_CNT__", str(default_parallel))
    return source.replace("__MAX_PARL_CNT__", str(maximum_parallel))


def save_dag_file(sbj_area_cd: str, source: str) -> Path:
    DAG_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    output_path = DAG_OUTPUT_ROOT / f"mig_{sbj_area_cd}.py"
    output_path.write_text(source, encoding="utf-8")
    return output_path


def render_dag_generator(areas: pd.DataFrame, enabled: bool) -> None:
    candidates = areas.loc[
        areas.up_sbj_area_cd.map(text).ne("")
        & areas.active_yn.fillna(False).astype(bool)
    ].copy()
    if candidates.empty:
        st.info("사용 중인 실행 주제영역이 없습니다.", icon=":material/info:")
        return
    selected_code = st.selectbox("실행 주제영역", candidates.sbj_area_cd.tolist(), key="dag_generator_area")
    current = candidates.loc[candidates.sbj_area_cd.eq(selected_code)].iloc[0]
    dag_id = f"mig_{text(current.sbj_area_cd).lower()}"
    default_parallel = 1 if pd.isna(current.dflt_parl_cnt) else int(current.dflt_parl_cnt)
    maximum_parallel = 1 if pd.isna(current.max_parl_cnt) else int(current.max_parl_cnt)
    try:
        source = dag_source(text(current.sbj_area_cd), text(current.up_sbj_area_cd), dag_id, default_parallel, maximum_parallel)
        compile(source, f"{dag_id}.py", "exec")
    except ValueError as error:
        st.error(str(error), icon=":material/error:")
        return
    st.success("Python 문법 검증 완료", icon=":material/check_circle:")
    st.code(source, language="python")
    if st.button("DAG 파일 생성", icon=":material/terminal:", type="primary", disabled=not enabled):
        output_path = save_dag_file(text(current.sbj_area_cd), source)
        st.success(f"생성 완료: {output_path.name}")
    st.download_button("DAG 파일 다운로드", data=source, file_name=f"mig_{text(current.sbj_area_cd)}.py", mime="text/x-python", icon=":material/download:")

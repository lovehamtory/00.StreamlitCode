from __future__ import annotations

import streamlit as st

from SrcTgtRuntime import qualified, query_frame, runtime_context


st.subheader(":material/history: 실행 이력")

try:
    context = runtime_context()
    query = f'''SELECT dag_nm, dag_run_id, mpg_id, task_nm, wrk_dvsn_cd, load_mthd_cd, wrk_sts_cd, src_row_cnt, tgt_row_cnt, s3_byte_size, s3_mnf_path, sql_file_path, src_where_cnd, wrk_stt_dtm, wrk_end_dtm, wrk_elps_sec, wrk_msg
                  FROM {qualified(context.schema_name, 'tb_mig_run_log')}
                 ORDER BY run_hist_id DESC
                 LIMIT 500'''
    labels = {"dag_nm": "DAG명", "dag_run_id": "DAG실행ID", "mpg_id": "테이블매핑ID", "task_nm": "태스크명", "wrk_dvsn_cd": "작업구분", "load_mthd_cd": "적재방식", "wrk_sts_cd": "작업상태", "src_row_cnt": "원천건수", "tgt_row_cnt": "대상건수", "s3_byte_size": "S3크기", "s3_mnf_path": "S3매니페스트", "sql_file_path": "SQL경로", "src_where_cnd": "원천조회조건", "wrk_stt_dtm": "작업시작일시", "wrk_end_dtm": "작업종료일시", "wrk_elps_sec": "실행경과초", "wrk_msg": "작업메시지"}
    st.dataframe(query_frame(context.values, query).rename(columns=labels), hide_index=True, height=600)
except Exception as error:
    st.error(f"실행 이력 조회 실패: {error}", icon=":material/error:")

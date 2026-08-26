from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Callable

import pandas as pd
import streamlit as st


def validation_history(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], start_dt: date, end_dt: date) -> pd.DataFrame:
    query = f'''SELECT V.vald_hist_id, V.exec_run_id, V.dag_nm, V.dag_run_id, V.mpg_id,
                      T.prj_cd, T.sbj_area_cd, T.src_sch_nm || '.' || T.src_tbl_nm AS src_tbl,
                      T.tgt_sch_nm || '.' || T.tgt_tbl_nm AS tgt_tbl, V.vald_dvsn_cd, V.s3_manf_path,
                      V.cnt_vald_sts_cd, V.src_cnt, V.tgt_cnt, V.cnt_diff, V.sum_vald_sts_cd,
                      V.hsh_vald_sts_cd, V.vald_sts_cd, V.vald_stt_dtm, V.vald_end_dtm,
                      V.vald_elps_sec, V.vald_msg
                  FROM {qualified(schema_name, "tb_mig_vald_rslt")} V
                  LEFT JOIN {qualified(schema_name, "tb_mig_tbl_mpg")} T ON T.mpg_id = V.mpg_id
                 WHERE CAST(V.vald_stt_dtm AS DATE) BETWEEN %s AND %s
                 ORDER BY V.vald_hist_id DESC'''
    return query_frame(values, query, (start_dt, end_dt))


def validation_columns(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], history_id: int) -> pd.DataFrame:
    query = f'''SELECT vald_item_cd, col_nm, src_val, tgt_val, diff_val, vald_sts_cd
                  FROM {qualified(schema_name, "tb_mig_vald_col_rslt")}
                 WHERE vald_hist_id = %s
                 ORDER BY vald_item_cd, col_nm'''
    return query_frame(values, query, (history_id,))


def render_validation(values: dict[str, Any], schema_name: str, query_frame: Callable[..., pd.DataFrame], qualified: Callable[[str, str], str]) -> None:
    controls = st.columns([1, 1, 2, 2])
    with controls[0]:
        start_dt = st.date_input("검증 시작일", value=date.today() - timedelta(days=7))
    with controls[1]:
        end_dt = st.date_input("검증 종료일", value=date.today())
    if end_dt < start_dt:
        st.error("검증 종료일은 검증 시작일보다 빠를 수 없습니다.", icon=":material/error:")
        return
    try:
        frame = validation_history(query_frame, values, schema_name, qualified, start_dt, end_dt)
    except Exception as error:
        st.error(f"검증이력 조회 실패: {error}", icon=":material/error:")
        return
    with controls[2]:
        division = st.selectbox("검증구분", ["전체", "SRC_S3", "S3_TGT"])
    with controls[3]:
        status = st.selectbox("검증상태", ["전체", "SUCCESS", "FAILED", "RUNNING"])
    filtered = frame.copy()
    if division != "전체":
        filtered = filtered.loc[filtered.vald_dvsn_cd.eq(division)]
    if status != "전체":
        filtered = filtered.loc[filtered.vald_sts_cd.eq(status)]
    summary = st.columns(4)
    summary[0].metric("검증 대상", f"{len(filtered):,}건")
    summary[1].metric("성공", f"{int(filtered.vald_sts_cd.eq('SUCCESS').sum()):,}건")
    summary[2].metric("실패", f"{int(filtered.vald_sts_cd.eq('FAILED').sum()):,}건")
    summary[3].metric("진행", f"{int(filtered.vald_sts_cd.eq('RUNNING').sum()):,}건")
    shown = filtered.rename(columns={"exec_run_id": "이관실행ID", "dag_nm": "DAG명", "dag_run_id": "DAG실행ID", "mpg_id": "테이블매핑ID", "prj_cd": "프로젝트코드", "sbj_area_cd": "주제영역코드", "src_tbl": "원천테이블", "tgt_tbl": "대상테이블", "vald_dvsn_cd": "검증구분", "s3_manf_path": "S3매니페스트", "cnt_vald_sts_cd": "건수검증", "src_cnt": "원천건수", "tgt_cnt": "대상건수", "cnt_diff": "건수차이", "sum_vald_sts_cd": "합계검증", "hsh_vald_sts_cd": "해시검증", "vald_sts_cd": "검증상태", "vald_stt_dtm": "검증시작일시", "vald_end_dtm": "검증종료일시", "vald_elps_sec": "검증경과초", "vald_msg": "검증메시지"})
    st.dataframe(shown, hide_index=True, height=430)
    if not filtered.empty:
        selected = st.selectbox("컬럼 검증결과", filtered.vald_hist_id.tolist(), format_func=lambda value: f"{value} · {filtered.loc[filtered.vald_hist_id.eq(value)].iloc[0].tgt_tbl}")
        try:
            details = validation_columns(query_frame, values, schema_name, qualified, int(selected))
            st.dataframe(details.rename(columns={"vald_item_cd": "검증항목", "col_nm": "컬럼명", "src_val": "원천값", "tgt_val": "대상값", "diff_val": "차이값", "vald_sts_cd": "검증상태"}), hide_index=True)
        except Exception as error:
            st.error(f"컬럼 검증결과 조회 실패: {error}", icon=":material/error:")

from __future__ import annotations

import pandas as pd
import streamlit as st

from SrcTgtRuntime import RuntimeContext, public_monitor_context, qualified, query_frame


def dag_runs(context: RuntimeContext) -> pd.DataFrame:
    return query_frame(context.values, f'''SELECT dag_exec_id, dag_nm, dag_run_id, dag_dvsn_cd, map_cnt, suc_cnt, run_cnt, err_cnt, wrk_sts_cd, wrk_stt_dtm, wrk_end_dtm, wrk_elps_sec, wrk_msg
                                             FROM {qualified(context.schema_name, 'vw_mig_dag_run_sum')}
                                            ORDER BY dag_exec_id DESC
                                            LIMIT 200''')


def latest_table_runs(context: RuntimeContext) -> pd.DataFrame:
    return query_frame(context.values, f'''WITH LAST_RUN AS (
                                                SELECT mpg_id, dag_nm, dag_run_id, task_nm, wrk_dvsn_cd, wrk_sts_cd, src_row_cnt, tgt_row_cnt, wrk_stt_dtm, wrk_end_dtm, wrk_elps_sec, wrk_msg,
                                                       ROW_NUMBER() OVER (PARTITION BY mpg_id ORDER BY run_hist_id DESC) AS row_no
                                                  FROM {qualified(context.schema_name, 'tb_mig_run_log')}
                                                 WHERE mpg_id IS NOT NULL
                                            )
                                            SELECT T.mpg_id, T.prj_cd, T.sbj_area_cd, T.src_sch_nm || '.' || T.src_tbl_nm AS src_tbl, T.tgt_sch_nm || '.' || T.tgt_tbl_nm AS tgt_tbl,
                                                   L.dag_nm, L.dag_run_id, L.wrk_dvsn_cd, L.wrk_sts_cd, L.src_row_cnt, L.tgt_row_cnt, L.wrk_stt_dtm, L.wrk_end_dtm, L.wrk_elps_sec, L.wrk_msg
                                              FROM {qualified(context.schema_name, 'tb_mig_tbl_mpg')} T
                                              LEFT JOIN LAST_RUN L ON L.mpg_id = T.mpg_id AND L.row_no = 1
                                             WHERE T.active_yn = TRUE
                                             ORDER BY T.sbj_area_cd, T.mpg_id''')


def summary_counts(frame: pd.DataFrame) -> tuple[int, int, int, int]:
    total = len(frame)
    completed = int(frame.wrk_sts_cd.fillna("").str.upper().eq("SUCCESS").sum())
    running = int(frame.wrk_sts_cd.fillna("").str.upper().isin({"RUNNING", "QUEUED", "STARTED"}).sum())
    errors = int(frame.wrk_sts_cd.fillna("").str.upper().eq("FAILED").sum())
    return total, completed, running, errors


def render_cards(frame: pd.DataFrame) -> None:
    total, completed, running, errors = summary_counts(frame)
    columns = st.columns(4)
    columns[0].metric("전체 테이블", f"{total:,}")
    columns[1].metric("완료", f"{completed:,}")
    columns[2].metric("진행중", f"{running:,}")
    columns[3].metric("오류", f"{errors:,}")
    st.progress(completed / total if total else 0.0, text=f"완료율 {completed / total:.2%}" if total else "완료율 0.00%")


def render_monitor(context: RuntimeContext) -> None:
    try:
        tables = latest_table_runs(context)
        runs = dag_runs(context)
    except Exception as error:
        st.error(f"실행 현황 조회 실패: {error}", icon=":material/error:")
        return
    render_cards(tables)
    st.subheader("DAG 실행")
    dag_display = runs.rename(columns={"dag_nm": "DAG명", "dag_run_id": "DAG실행ID", "dag_dvsn_cd": "DAG구분", "map_cnt": "전체", "suc_cnt": "완료", "run_cnt": "진행중", "err_cnt": "오류", "wrk_sts_cd": "상태", "wrk_stt_dtm": "시작일시", "wrk_end_dtm": "종료일시", "wrk_elps_sec": "경과초", "wrk_msg": "메시지"})
    st.dataframe(dag_display.drop(columns=["dag_exec_id"]), hide_index=True, height=300, column_config={"전체": st.column_config.NumberColumn(format="localized"), "완료": st.column_config.NumberColumn(format="localized"), "진행중": st.column_config.NumberColumn(format="localized"), "오류": st.column_config.NumberColumn(format="localized")})
    st.subheader("테이블 실행")
    table_display = tables.rename(columns={"mpg_id": "테이블매핑ID", "prj_cd": "프로젝트코드", "sbj_area_cd": "주제영역코드", "src_tbl": "원천테이블", "tgt_tbl": "대상테이블", "dag_nm": "DAG명", "dag_run_id": "DAG실행ID", "wrk_dvsn_cd": "작업구분", "wrk_sts_cd": "상태", "src_row_cnt": "원천건수", "tgt_row_cnt": "대상건수", "wrk_stt_dtm": "시작일시", "wrk_end_dtm": "종료일시", "wrk_elps_sec": "경과초", "wrk_msg": "메시지"})
    st.dataframe(table_display.drop(columns=["메시지"]), hide_index=True, height=420, column_config={"원천건수": st.column_config.NumberColumn(format="localized"), "대상건수": st.column_config.NumberColumn(format="localized"), "경과초": st.column_config.NumberColumn(format="localized")})


try:
    monitor_context = public_monitor_context()
except Exception:
    st.title("📈 실행 현황")
    st.caption("⚙️ Created by ♡홍율파파♡")
    st.info("초기 설정 메뉴에서 메타 연결과 스키마를 준비한 뒤 다시 선택하십시오.", icon=":material/settings:")
else:
    st.title("📈 실행 현황")
    st.caption("⚙️ Created by ♡홍율파파♡")

    @st.fragment(run_every="5s")
    def auto_refresh() -> None:
        render_monitor(monitor_context)

    auto_refresh()

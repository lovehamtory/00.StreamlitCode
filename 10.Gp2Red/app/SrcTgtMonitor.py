from __future__ import annotations

import pandas as pd
import streamlit as st

from SrcTgtRuntime import RuntimeContext, public_monitor_context, qualified, query_frame, text


def progress_frame(context: RuntimeContext) -> pd.DataFrame:
    query = f'''WITH LAST_LOG AS (
                    SELECT mpg_id, wrk_sts_cd, wrk_stt_dtm, wrk_end_dtm, wrk_elps_sec, src_row_cnt, tgt_row_cnt, wrk_msg,
                           ROW_NUMBER() OVER (PARTITION BY mpg_id ORDER BY wrk_stt_dtm DESC, run_hist_id DESC) AS row_no
                      FROM {qualified(context.schema_name, "tb_mig_run_log")}
                     WHERE mpg_id IS NOT NULL
                )
                SELECT T.mpg_id AS "MPG_ID", T.prj_cd AS "PRJ_CD", T.sbj_area_cd AS "SBJ_AREA_CD", COALESCE(S.up_sbj_area_cd, T.sbj_area_cd) AS "UP_SBJ_AREA_CD", T.src_sch_nm || '.' || T.src_tbl_nm AS "SRC_TBL", T.tgt_sch_nm || '.' || T.tgt_tbl_nm AS "TGT_TBL", L.wrk_sts_cd AS "WRK_STS_CD", L.src_row_cnt AS "RUN_SRC_ROW_CNT", L.tgt_row_cnt AS "TGT_ROW_CNT", L.wrk_stt_dtm AS "WRK_STT_DTM", L.wrk_end_dtm AS "WRK_END_DTM", L.wrk_elps_sec AS "WRK_ELPS_SEC", L.wrk_msg AS "WRK_MSG"
                  FROM {qualified(context.schema_name, "tb_mig_tbl_mpg")} T
                  LEFT JOIN {qualified(context.schema_name, "tb_mig_sbj_area")} S ON S.sbj_area_cd = T.sbj_area_cd
                  LEFT JOIN LAST_LOG L ON L.mpg_id = T.mpg_id AND L.row_no = 1
                 WHERE T.active_yn = TRUE
                 ORDER BY T.prj_cd, T.sbj_area_cd, T.tgt_sch_nm, T.tgt_tbl_nm'''
    return query_frame(context.values, query)


def status_counts(frame: pd.DataFrame) -> tuple[int, int, int, int]:
    total = len(frame)
    completed = int(frame.WRK_STS_CD.fillna("").str.upper().isin({"SUCCESS", "COMPLETED"}).sum())
    failed = int(frame.WRK_STS_CD.fillna("").str.upper().eq("FAILED").sum())
    running = int(frame.WRK_STS_CD.fillna("").str.upper().isin({"RUNNING", "REQUESTED", "STARTED"}).sum())
    return total, completed, failed, running


def progress_card(title: str, frame: pd.DataFrame) -> None:
    total, completed, failed, running = status_counts(frame)
    rate = completed / total if total else 0.0
    with st.container(border=True):
        st.markdown(f"#### {title}")
        st.metric("완료 / 전체", f"{completed:,} / {total:,}", f"{rate:.2%}")
        st.progress(rate)
        st.caption(f"진행 {running:,} · 실패 {failed:,}")


def dag_frame(context: RuntimeContext, visible_maps: pd.DataFrame) -> pd.DataFrame:
    if visible_maps.empty:
        return pd.DataFrame(columns=["DAG명", "DAG실행ID", "테이블", "성공", "실패", "작업상태", "작업시작일시", "작업종료일시", "실행경과초"])
    query = f'''SELECT dag_nm AS "DAG명", dag_run_id AS "DAG실행ID", COUNT(DISTINCT mpg_id) AS "테이블", SUM(CASE WHEN wrk_sts_cd = 'SUCCESS' THEN 1 ELSE 0 END) AS "성공", SUM(CASE WHEN wrk_sts_cd = 'FAILED' THEN 1 ELSE 0 END) AS "실패", CASE WHEN SUM(CASE WHEN wrk_sts_cd = 'FAILED' THEN 1 ELSE 0 END) > 0 THEN 'FAILED' WHEN SUM(CASE WHEN wrk_sts_cd IN ('RUNNING', 'REQUESTED', 'STARTED') THEN 1 ELSE 0 END) > 0 THEN 'RUNNING' WHEN SUM(CASE WHEN wrk_sts_cd = 'SUCCESS' THEN 1 ELSE 0 END) > 0 THEN 'SUCCESS' ELSE 'PENDING' END AS "작업상태", MIN(wrk_stt_dtm) AS "작업시작일시", MAX(wrk_end_dtm) AS "작업종료일시", DATEDIFF(second, MIN(wrk_stt_dtm), MAX(COALESCE(wrk_end_dtm, wrk_stt_dtm))) AS "실행경과초"
                  FROM {qualified(context.schema_name, "tb_mig_run_log")}
                 GROUP BY dag_nm, dag_run_id
                 ORDER BY MIN(wrk_stt_dtm) DESC
                 LIMIT 100'''
    return query_frame(context.values, query)


def render_monitor(context: RuntimeContext) -> None:
    try:
        progress = progress_frame(context)
    except Exception:
        st.error("이관 현황을 조회할 수 없습니다.", icon=":material/error:")
        return
    st.subheader("📊 진행 현황")
    cards = [("전체", progress)]
    for area_code in sorted(code for code in progress.UP_SBJ_AREA_CD.dropna().map(text).unique() if code):
        cards.append((area_code, progress.loc[progress.UP_SBJ_AREA_CD.eq(area_code)]))
    for start in range(0, len(cards), 4):
        columns = st.columns(4)
        for column, (title, frame) in zip(columns, cards[start:start + 4]):
            with column:
                progress_card(title, frame)
    st.subheader("📋 테이블 이관 현황")
    display = progress.rename(columns={"UP_SBJ_AREA_CD": "상위 주제영역", "SBJ_AREA_CD": "주제영역", "SRC_TBL": "원천 테이블", "TGT_TBL": "대상 테이블", "WRK_STS_CD": "작업 상태", "RUN_SRC_ROW_CNT": "원천 처리 건수", "TGT_ROW_CNT": "대상 처리 건수", "WRK_STT_DTM": "작업 시작일시", "WRK_END_DTM": "작업 종료일시", "WRK_ELPS_SEC": "실행 경과초"})
    st.dataframe(display.drop(columns=["MPG_ID", "PRJ_CD", "WRK_MSG"]), hide_index=True, height=430, column_config={"원천 처리 건수": st.column_config.NumberColumn(format="localized"), "대상 처리 건수": st.column_config.NumberColumn(format="localized"), "실행 경과초": st.column_config.NumberColumn(format="localized")})
    st.subheader("⚡ 최근 DAG 실행")
    try:
        st.dataframe(dag_frame(context, progress), hide_index=True, height=320, column_config={"테이블": st.column_config.NumberColumn(format="localized"), "성공": st.column_config.NumberColumn(format="localized"), "실패": st.column_config.NumberColumn(format="localized"), "실행경과초": st.column_config.NumberColumn(format="localized")})
    except Exception:
        st.info("DAG 실행 이력이 없습니다.", icon=":material/info:")

def main() -> None:
    try:
        context = public_monitor_context()
    except Exception:
        st.error("이관 현황 연결을 확인할 수 없습니다.", icon=":material/error:")
        st.stop()
    st.title("📈 이관 현황")
    st.caption("⚙️ Created by ♡홍율파파")

    @st.fragment(run_every="30s")
    def auto_refresh() -> None:
        render_monitor(context)

    auto_refresh()


if __name__ == "__main__":
    main()

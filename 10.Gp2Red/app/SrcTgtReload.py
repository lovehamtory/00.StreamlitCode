from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from SrcTgtMapping import render_one_time_execution
from SrcTgtRuntime import connect, qualified, query_frame, runtime_context


def table_maps(values: dict[str, Any], schema_name: str) -> pd.DataFrame:
    query = f"""
        SELECT mpg_id, prj_cd, sbj_area_cd, src_sch_nm, src_tbl_nm, tgt_sch_nm, tgt_tbl_nm, load_sts_cd
          FROM {qualified(schema_name, 'tb_mig_tbl_mpg')}
         WHERE active_yn = TRUE
         ORDER BY prj_cd, sbj_area_cd, tgt_sch_nm, tgt_tbl_nm, mpg_id
    """
    return query_frame(values, query)


context = runtime_context()

st.title("🔁 일회성 이관 실행")
st.caption("적재상태를 바꾸지 않고 선택 테이블만 전용 DAG로 S3 추출·검증 또는 대상 적재·검증합니다.")

try:
    maps = table_maps(context.values, context.schema_name)
    render_one_time_execution(
        maps,
        context.values,
        context.schema_name,
        lambda project, subject_area: True,
        query_frame,
        connect,
        qualified,
    )
except Exception as error:
    st.error(f"일회성 이관 실행 화면을 열 수 없습니다: {error}", icon=":material/error:")

st.caption("⚙️ Created by ♡홍율파파")

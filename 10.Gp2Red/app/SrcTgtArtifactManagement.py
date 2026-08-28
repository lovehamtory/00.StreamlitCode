from __future__ import annotations

import streamlit as st

from SrcTgtArtifact import render_artifacts
from SrcTgtRuntime import connect, qualified, query_frame, runtime_context


st.subheader(":material/inventory_2: 산출물 관리")

try:
    context = runtime_context()
except Exception:
    st.info("초기 설정 메뉴에서 메타 연결과 스키마를 준비한 뒤 다시 선택하십시오.", icon=":material/settings:")
    st.stop()

render_artifacts(context.values, context.schema_name, query_frame, connect, qualified)

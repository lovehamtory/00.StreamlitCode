from __future__ import annotations

import streamlit as st

from SrcTgtConnection import render_connection_management
from SrcTgtRuntime import connect, qualified, query_frame, runtime_context


st.subheader(":material/cable: 접속정보")

try:
    context = runtime_context()
    render_connection_management(context.values, context.schema_name, query_frame, connect, qualified)
except Exception as error:
    if "No secrets found" in str(error) or "초기 설정용 Redshift 연결 설정" in str(error):
        st.info("초기 설정에서 메타 DB 연결을 먼저 준비하십시오.", icon=":material/settings:")
    else:
        st.error(f"접속정보 조회 실패: {error}", icon=":material/error:")

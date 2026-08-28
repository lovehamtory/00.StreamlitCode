from __future__ import annotations

import streamlit as st

from SrcTgtEmr import render_emr_management
from SrcTgtRuntime import qualified, query_frame, runtime_context


st.subheader(":material/computer: EMR")

try:
    context = runtime_context()
    render_emr_management(context.values, context.schema_name, query_frame, qualified)
except Exception as error:
    st.error(f"EMR 조회 실패: {error}", icon=":material/error:")

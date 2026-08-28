from __future__ import annotations

import streamlit as st

from SrcTgtRuntime import qualified, query_frame, runtime_context
from SrcTgtValidation import render_validation


st.subheader(":material/fact_check: 검증")

try:
    context = runtime_context()
    render_validation(context.values, context.schema_name, query_frame, qualified)
except Exception as error:
    st.error(f"검증 조회 실패: {error}", icon=":material/error:")

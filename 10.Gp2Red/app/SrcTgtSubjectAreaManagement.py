from __future__ import annotations

import streamlit as st

from SrcTgtRuntime import runtime_context
from SrcTgtSubjectArea import render_subject_area


st.subheader(":material/folder: 주제영역")

try:
    context = runtime_context()
    render_subject_area(context.values, context.schema_name)
except Exception as error:
    st.error(f"주제영역 조회 실패: {error}", icon=":material/error:")

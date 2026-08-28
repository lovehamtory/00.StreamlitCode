from __future__ import annotations

import streamlit as st

from SrcTgtRuntime import runtime_context
from SrcTgtTargetReflection import render_target_reflection


st.subheader(":material/data_object: 대상 DDL")

try:
    render_target_reflection(runtime_context())
except Exception as error:
    st.error(f"대상 DDL 조회 실패: {error}", icon=":material/error:")

from __future__ import annotations

import streamlit as st

from SrcTgtMapping import render_mapping_workspace
from SrcTgtRuntime import connect, qualified, query_frame, runtime_context
from SrcTgtTableMap import table_maps


st.subheader(":material/link: SRC·TGT 매핑")

try:
    context = runtime_context()
    render_mapping_workspace(table_maps(context.values, context.schema_name), context.values, context.schema_name, query_frame, connect, qualified)
except Exception as error:
    st.error(f"매핑 조회 실패: {error}", icon=":material/error:")

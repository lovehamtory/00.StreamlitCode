from __future__ import annotations

import streamlit as st

from SrcTgtDagGenerator import render_dag_generator
from SrcTgtRuntime import connect, qualified, query_frame, runtime_context
from SrcTgtSubjectArea import subject_areas
from SrcTgtTableMap import table_maps


st.subheader(":material/account_tree: DAG 생성")

try:
    context = runtime_context()
    render_dag_generator(subject_areas(context.values, context.schema_name), table_maps(context.values, context.schema_name), context.values, context.schema_name, query_frame, connect, qualified)
except Exception as error:
    st.error(f"DAG 생성 정보 조회 실패: {error}", icon=":material/error:")

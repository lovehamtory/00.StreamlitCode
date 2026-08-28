from __future__ import annotations

import streamlit as st
from SrcTgtConnection import connection_frame, connection_label, selectable_connections
from SrcTgtLayoutCommon import comparison, dates
from SrcTgtRuntime import qualified, query_frame, runtime_context, text


st.subheader(":material/difference: 테이블 변경 비교")

try:
    context = runtime_context()
    connections = connection_frame(query_frame, context.values, context.schema_name, qualified, active_only=True)
    sources = selectable_connections(connections)
    sources = sources.loc[sources.dbms_cd.map(text).str.upper().eq("GREENPLUM")]
    if sources.empty:
        raise ValueError("Greenplum 원천 접속정보를 등록하십시오.")
    connection_id = st.selectbox("원천 접속", sources.conn_id.tolist(), format_func=lambda item: connection_label(sources, item))
    available = dates(context.values, context.schema_name, connection_id)
    if len(available) < 2:
        raise ValueError("비교할 기준일이 두 건 이상 필요합니다.")
    before = st.selectbox("이전 기준일", available, index=len(available) - 2)
    after = st.selectbox("비교 기준일", available, index=len(available) - 1)
    if st.button("비교", type="primary", icon=":material/compare_arrows:"):
        if before == after:
            raise ValueError("서로 다른 기준일을 선택하십시오.")
        tables, columns = comparison(context.values, context.schema_name, connection_id, before, after)
        st.dataframe(tables.rename(columns={"src_sch_nm": "원천스키마", "src_tbl_nm": "원천테이블", "CHG_DVSN": "변경구분", "COL_CNT": "변경컬럼수"}), hide_index=True)
        st.dataframe(columns.rename(columns={"src_sch_nm": "원천스키마", "src_tbl_nm": "원천테이블", "src_col_no": "원천컬럼순번", "CHG_DVSN": "변경구분"}), hide_index=True, height=420)
except Exception as error:
    st.error(f"변경 비교 실패: {error}", icon=":material/error:")

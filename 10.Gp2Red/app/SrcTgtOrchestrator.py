from pathlib import Path

import streamlit as st


ROOT = Path(__file__).parent

st.set_page_config(page_title="이관 관리", page_icon=":material/account_tree:", layout="wide")

page = st.navigation(
    {
        "기준정보": [
            st.Page(ROOT / "SrcTgtReference.py", title="접속정보", icon=":material/cable:"),
            st.Page(ROOT / "SrcTgtSubjectAreaManagement.py", title="주제영역", icon=":material/folder:"),
        ],
        "이관관리": [
            st.Page(ROOT / "SrcTgtLayoutHistory.py", title="테이블 레이아웃", icon=":material/table_chart:"),
            st.Page(ROOT / "SrcTgtLayoutCompare.py", title="테이블 변경 비교", icon=":material/difference:"),
            st.Page(ROOT / "SrcTgtTargetDdl.py", title="대상 DDL", icon=":material/data_object:"),
            st.Page(ROOT / "SrcTgtControl.py", title="SRC·TGT 매핑", icon=":material/link:"),
            st.Page(ROOT / "SrcTgtDagManagement.py", title="DAG 생성", icon=":material/account_tree:"),
        ],
        "운영": [
            st.Page(ROOT / "SrcTgtMonitor.py", title="실행 현황", icon=":material/monitoring:"),
            st.Page(ROOT / "SrcTgtValidationManagement.py", title="검증", icon=":material/fact_check:"),
            st.Page(ROOT / "SrcTgtRunHistory.py", title="실행 이력", icon=":material/history:"),
            st.Page(ROOT / "SrcTgtEmrManagement.py", title="EMR", icon=":material/computer:"),
            st.Page(ROOT / "SrcTgtSnapshotRestore.py", title="스냅샷 복구", icon=":material/restore_page:"),
        ],
        "산출물": [
            st.Page(ROOT / "SrcTgtArtifactManagement.py", title="산출물 관리", icon=":material/inventory_2:"),
        ],
        "설정": [
            st.Page(ROOT / "SrcTgtInitialize.py", title="초기 설정", icon=":material/settings:"),
        ],
    },
    position="sidebar",
    expanded=True,
)

with st.sidebar:
    st.space("medium")
    st.caption("⚙️ Created by ♡홍율파파♡", text_alignment="center")

page.run()

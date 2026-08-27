from pathlib import Path

import streamlit as st


ROOT = Path(__file__).parent

st.set_page_config(page_title="이관 관리", page_icon=":material/account_tree:", layout="wide")

page = st.navigation(
    {
        "이관": [
            st.Page(ROOT / "SrcTgtControl.py", title="이관 관리", icon=":material/account_tree:"),
            st.Page(ROOT / "SrcTgtLayoutHistory.py", title="구조·변경", icon=":material/difference:"),
        ],
        "운영": [
            st.Page(ROOT / "SrcTgtMonitor.py", title="실행 현황", icon=":material/monitoring:"),
            st.Page(ROOT / "SrcTgtSnapshotRestore.py", title="스냅샷 복구", icon=":material/restore_page:"),
        ],
        "설정": [
            st.Page(ROOT / "SrcTgtInitialize.py", title="초기 설정", icon=":material/settings:"),
        ],
    },
    position="sidebar",
)

with st.sidebar:
    st.divider()
    st.caption("⚙️ Created by ♡홍율파파♡")

page.run()

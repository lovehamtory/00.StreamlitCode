from pathlib import Path

import streamlit as st

from SrcTgtSecurity import logout, require_access
from SrcTgtSetup import configured_schema, connection_values, metadata_ready, render_initial_setup


ROOT = Path(__file__).parent

st.set_page_config(page_title="이관 관리", page_icon=":material/account_tree:", layout="wide")

try:
    initial_schema = configured_schema()
    initialized = bool(initial_schema) and metadata_ready(connection_values(), initial_schema)
except Exception:
    initialized = False

if not initialized:
    render_initial_setup()
    st.stop()

access = require_access()

with st.sidebar:
    st.caption(f"로그인 사용자: {access.user_id}")
    if st.button("로그아웃", icon=":material/logout:", key="orchestrator_logout"):
        logout()

page = st.navigation(
    {
        "이관": [
            st.Page(ROOT / "SrcTgtControl.py", title="이관 관리", icon=":material/account_tree:"),
        ],
        "원천": [
            st.Page(ROOT / "SrcTgtLayoutHistory.py", title="원천 레이아웃", icon=":material/difference:"),
        ],
    },
    position="sidebar",
)

page.run()


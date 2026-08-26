from pathlib import Path

import streamlit as st

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

page = st.navigation(
    {
        "이관": [
            st.Page(ROOT / "SrcTgtControl.py", title="이관 관리", icon=":material/account_tree:"),
            st.Page(ROOT / "SrcTgtReload.py", title="일회성 이관 실행", icon=":material/restart_alt:"),
        ],
        "운영": [
            st.Page(ROOT / "SrcTgtSnapshotRestore.py", title="스냅샷 복구", icon=":material/restore_page:"),
        ],
        "구조": [
            st.Page(ROOT / "SrcTgtLayoutHistory.py", title="구조조회", icon=":material/difference:"),
        ],
    },
    position="sidebar",
)

page.run()

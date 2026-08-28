from pathlib import Path

import streamlit as st

from SrcTgtSecurity import allowed, auth_ready, clear_login, current_user, permissions, render_login, render_password_change, save_login
from SrcTgtSetup import configured_schema, connection_values


ROOT = Path(__file__).parent

st.set_page_config(page_title="이관 관리", page_icon=":material/account_tree:", layout="wide")


def page_specs() -> list[tuple[str, str, str, str, str]]:
    return [
        ("이관관리", "LAYOUT", "SrcTgtLayoutHistory.py", "테이블 레이아웃", ":material/table_chart:"),
        ("이관관리", "DDL", "SrcTgtTargetDdl.py", "대상 DDL", ":material/data_object:"),
        ("이관관리", "MPG", "SrcTgtControl.py", "SRC·TGT 매핑", ":material/link:"),
        ("이관관리", "DAG", "SrcTgtDagManagement.py", "DAG 생성", ":material/account_tree:"),
        ("운영", "MON", "SrcTgtMonitor.py", "실행 현황", ":material/monitoring:"),
        ("운영", "CMP", "SrcTgtLayoutCompare.py", "테이블 변경 비교", ":material/difference:"),
        ("운영", "VALD", "SrcTgtValidationManagement.py", "검증", ":material/fact_check:"),
        ("운영", "RUN", "SrcTgtRunHistory.py", "실행 이력", ":material/history:"),
        ("운영", "EMR", "SrcTgtEmrManagement.py", "EMR", ":material/computer:"),
        ("운영", "SNAP", "SrcTgtSnapshotRestore.py", "스냅샷 복구", ":material/restore_page:"),
        ("산출물", "ARTF", "SrcTgtArtifactManagement.py", "산출물 관리", ":material/inventory_2:"),
        ("기준정보", "CONN", "SrcTgtReference.py", "접속정보", ":material/cable:"),
        ("기준정보", "SBJ", "SrcTgtSubjectAreaManagement.py", "주제영역", ":material/folder:"),
        ("설정", "USR", "SrcTgtUserManagement.py", "사용자 관리", ":material/group:"),
        ("설정", "AUTH", "SrcTgtPermissionManagement.py", "권한 관리", ":material/admin_panel_settings:"),
        ("설정", "INIT", "SrcTgtInitialize.py", "초기 설정", ":material/settings:"),
    ]


schema_name = configured_schema()
values = None
metadata_available = False
try:
    values = connection_values()
    metadata_available = bool(schema_name) and auth_ready(values, schema_name)
except Exception:
    metadata_available = False

user = current_user()
if metadata_available and user is not None and bool(user.get("bootstrap")):
    clear_login()
    user = None
elif metadata_available and user is not None:
    try:
        user["permissions"] = permissions(values, schema_name, str(user["usr_id"]))
        save_login(user)
    except Exception:
        clear_login()
        user = None

if user is None:
    render_login(values, schema_name, metadata_available)
    st.stop()

if metadata_available and bool(user.get("pwd_chg_yn")):
    render_password_change(values, schema_name, user)
    st.stop()

if metadata_available:
    navigation: dict[str, list[st.Page]] = {}
    menu_by_title: dict[str, str] = {}
    for group, menu_code, source, title, icon in page_specs():
        if allowed(menu_code):
            navigation.setdefault(group, []).append(st.Page(ROOT / source, title=title, icon=icon))
            menu_by_title[title] = menu_code
    if not navigation:
        st.error("접근 권한이 있는 메뉴가 없습니다. 관리자에게 권한그룹을 요청하십시오.", icon=":material/error:")
        st.stop()
else:
    navigation = {"설정": [st.Page(ROOT / "SrcTgtInitialize.py", title="초기 설정", icon=":material/settings:")]}
    menu_by_title = {"초기 설정": "INIT"}

page = st.navigation(navigation, position="sidebar", expanded=True)
st.session_state["mig_active_menu"] = menu_by_title.get(page.title, "")

with st.sidebar:
    st.caption(f":material/person: {user['usr_nm']} ({user['usr_id']})", text_alignment="center")
    if st.button("로그아웃", icon=":material/logout:", width="stretch"):
        clear_login()
        st.rerun()
    st.space("medium")
    st.caption("⚙️ Created by ♡홍율파파♡", text_alignment="center")

page.run()

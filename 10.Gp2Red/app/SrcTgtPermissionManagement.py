from __future__ import annotations

import streamlit as st

from SrcTgtRuntime import runtime_context, text
from SrcTgtSecurity import group_frame, permission_frame, require_save, save_group


st.subheader(":material/admin_panel_settings: 권한 관리")

try:
    context = runtime_context()
    groups = group_frame(context.values, context.schema_name)
    st.dataframe(groups.rename(columns={"auth_grp_id": "권한그룹 ID", "auth_grp_nm": "권한그룹명", "use_yn": "사용", "crt_dtm": "등록일시", "upd_dtm": "수정일시"}), hide_index=True, height=220)
    mode = st.segmented_control("권한 관리 업무", ["권한그룹 등록", "권한그룹 수정"], default="권한그룹 등록", label_visibility="collapsed")
    selected = None
    if mode == "권한그룹 수정":
        if groups.empty:
            st.info("등록된 권한그룹이 없습니다.", icon=":material/info:")
            st.stop()
        selected_id = st.selectbox("권한그룹", groups.auth_grp_id.tolist(), format_func=lambda value: f"{value} · {text(groups.loc[groups.auth_grp_id.eq(value)].iloc[0].auth_grp_nm)}")
        selected = groups.loc[groups.auth_grp_id.eq(selected_id)].iloc[0]
    permissions = permission_frame(context.values, context.schema_name, "" if selected is None else text(selected.auth_grp_id))
    with st.form("mig_permission_form"):
        item_id = st.text_input("권한그룹 ID", value="" if selected is None else text(selected.auth_grp_id), disabled=selected is not None)
        item_name = st.text_input("권한그룹명", value="" if selected is None else text(selected.auth_grp_nm))
        used = st.toggle("사용", value=True if selected is None else bool(selected.use_yn))
        edited = st.data_editor(permissions[["메뉴", "read_yn", "save_yn"]], hide_index=True, column_config={"메뉴": st.column_config.TextColumn("메뉴", disabled=True), "read_yn": st.column_config.CheckboxColumn("조회"), "save_yn": st.column_config.CheckboxColumn("저장")}, width="stretch")
        saved = st.form_submit_button("권한그룹 저장", type="primary", icon=":material/save:")
    if saved:
        try:
            require_save("AUTH")
            input_permissions = permissions[["menu_cd"]].copy()
            input_permissions["read_yn"] = edited["read_yn"]
            input_permissions["save_yn"] = edited["save_yn"]
            save_group(context.values, context.schema_name, {"auth_grp_id": item_id, "auth_grp_nm": item_name, "use_yn": used}, input_permissions, selected is None)
            st.success("권한그룹을 저장했습니다.", icon=":material/check_circle:")
            st.rerun()
        except Exception as error:
            st.error(f"권한그룹 저장 실패: {error}", icon=":material/error:")
except Exception as error:
    st.error(f"권한 관리 조회 실패: {error}", icon=":material/error:")

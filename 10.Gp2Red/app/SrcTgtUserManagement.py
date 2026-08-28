from __future__ import annotations

import pandas as pd
import streamlit as st

from SrcTgtRuntime import runtime_context, text
from SrcTgtSecurity import group_frame, require_save, save_user, user_frame


st.subheader(":material/group: 사용자 관리")

try:
    context = runtime_context()
    users = user_frame(context.values, context.schema_name)
    groups = group_frame(context.values, context.schema_name)
    st.dataframe(users.rename(columns={"usr_id": "사용자 ID", "usr_nm": "사용자명", "use_yn": "사용", "pwd_chg_yn": "비밀번호 변경", "vald_stt_dt": "유효시작일", "vald_end_dt": "유효종료일", "auth_grp_ids": "권한그룹", "upd_dtm": "수정일시"}), hide_index=True, height=260)
    mode = st.segmented_control("사용자 관리 업무", ["사용자 등록", "사용자 수정"], default="사용자 등록", label_visibility="collapsed")
    selected = None
    if mode == "사용자 수정":
        if users.empty:
            st.info("등록된 사용자가 없습니다.", icon=":material/info:")
            st.stop()
        selected_id = st.selectbox("사용자", users.usr_id.tolist(), format_func=lambda value: f"{value} · {text(users.loc[users.usr_id.eq(value)].iloc[0].usr_nm)}")
        selected = users.loc[users.usr_id.eq(selected_id)].iloc[0]
    group_ids = groups.loc[groups.use_yn.fillna(False).astype(bool), "auth_grp_id"].map(text).tolist()
    selected_groups = [] if selected is None else [value.strip() for value in text(selected.auth_grp_ids).split(",") if value.strip()]
    with st.form("mig_user_form"):
        login_id = st.text_input("사용자 ID", value="" if selected is None else text(selected.usr_id), disabled=selected is not None)
        user_name = st.text_input("사용자명", value="" if selected is None else text(selected.usr_nm))
        assigned_groups = st.multiselect("권한그룹", group_ids, default=[value for value in selected_groups if value in group_ids])
        used = st.toggle("사용", value=True if selected is None else bool(selected.use_yn))
        validity = st.toggle("유효기간 적용", value=False if selected is None else pd.notna(selected.vald_stt_dt) or pd.notna(selected.vald_end_dt))
        start_date = st.date_input("유효시작일", value=None if selected is None or pd.isna(selected.vald_stt_dt) else selected.vald_stt_dt, disabled=not validity)
        end_date = st.date_input("유효종료일", value=None if selected is None or pd.isna(selected.vald_end_dt) else selected.vald_end_dt, disabled=not validity)
        saved = st.form_submit_button("사용자 저장", type="primary", icon=":material/save:")
    if saved:
        try:
            require_save("USR")
            if validity and start_date and end_date and start_date > end_date:
                raise ValueError("유효시작일은 유효종료일보다 늦을 수 없습니다.")
            save_user(context.values, context.schema_name, {"usr_id": login_id, "usr_nm": user_name, "use_yn": used, "vald_stt_dt": start_date if validity else None, "vald_end_dt": end_date if validity else None}, assigned_groups, selected is None)
            st.success("사용자를 저장했습니다. 신규 사용자의 초기 비밀번호는 사용자 ID와 같으며 첫 로그인에서 변경해야 합니다.", icon=":material/check_circle:")
            st.rerun()
        except Exception as error:
            st.error(f"사용자 저장 실패: {error}", icon=":material/error:")
except Exception as error:
    st.error(f"사용자 관리 조회 실패: {error}", icon=":material/error:")

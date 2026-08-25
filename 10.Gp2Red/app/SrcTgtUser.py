from __future__ import annotations

from typing import Any, Callable

import pandas as pd
import streamlit as st

from SrcTgtSecurity import password_hash, text


GROUP_ROLES = {
    "관리자": ["ADMIN"],
    "팀원": ["READ", "EDIT", "APRV", "EXEC"],
    "기타": ["READ"],
}


def user_frame(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str]) -> pd.DataFrame:
    query = f'''SELECT usr_id, usr_nm, pwd_chg_req_yn, active_yn, last_login_dtm, pwd_chg_dtm, crt_dtm, upd_dtm
                  FROM {qualified(schema_name, "tb_mig_usr")}
                 ORDER BY usr_id'''
    return query_frame(values, query)


def authorization_frame(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], user_id: str) -> pd.DataFrame:
    query = f'''SELECT auth_role_cd, prj_cd, sbj_area_cd, active_yn
                  FROM {qualified(schema_name, "tb_mig_usr_auth")}
                 WHERE usr_id = %s
                 ORDER BY auth_role_cd, prj_cd, sbj_area_cd'''
    return query_frame(values, query, (user_id,))


def ensure_user_id(user_id: str) -> str:
    value = text(user_id)
    if not value or len(value) > 320 or "\x00" in value:
        raise ValueError("사용자 ID는 1~320자로 입력하십시오.")
    return value


def group_from_authorizations(frame: pd.DataFrame) -> str:
    active_roles = {text(value).upper() for value in frame.loc[frame.active_yn.eq(True), "auth_role_cd"].tolist()} if not frame.empty else set()
    if "ADMIN" in active_roles:
        return "관리자"
    if active_roles.intersection({"EDIT", "APRV", "EXEC"}):
        return "팀원"
    return "기타"


def first_scope(frame: pd.DataFrame) -> tuple[str, str]:
    if frame.empty:
        return "", ""
    active = frame.loc[frame.active_yn.eq(True)]
    current = active.iloc[0] if not active.empty else frame.iloc[0]
    return text(current.prj_cd), text(current.sbj_area_cd)


def save_new_user(connect: Callable[[dict[str, Any]], Any], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], user_id: str, user_name: str, group: str, project: str, subject: str, admin_id: str) -> None:
    normalized_id = ensure_user_id(user_id)
    if group not in GROUP_ROLES:
        raise ValueError("권한 그룹이 올바르지 않습니다.")
    user_table = qualified(schema_name, "tb_mig_usr")
    auth_table = qualified(schema_name, "tb_mig_usr_auth")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT usr_id FROM {user_table} WHERE usr_id = %s", (normalized_id,))
            if cursor.fetchall():
                raise ValueError("이미 등록된 사용자 ID입니다.")
            cursor.execute(f"INSERT INTO {user_table} (usr_id, usr_nm, pwd_hsh_val, pwd_chg_req_yn, active_yn, crt_by, upd_by) VALUES (%s, %s, %s, TRUE, TRUE, %s, %s)", (normalized_id, text(user_name) or normalized_id, password_hash(normalized_id), admin_id, admin_id))
            for role in GROUP_ROLES[group]:
                cursor.execute(f"INSERT INTO {auth_table} (usr_id, auth_role_cd, prj_cd, sbj_area_cd, active_yn, crt_by, upd_by) VALUES (%s, %s, %s, %s, TRUE, %s, %s)", (normalized_id, role, text(project) or None, text(subject) or None, admin_id, admin_id))
        connection.commit()


def save_user_detail(connect: Callable[[dict[str, Any]], Any], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], selected_id: str, user_name: str, active: bool, group: str, project: str, subject: str, admin_id: str) -> None:
    if group not in GROUP_ROLES:
        raise ValueError("권한 그룹이 올바르지 않습니다.")
    if selected_id == admin_id and not active:
        raise ValueError("로그인한 관리자는 사용 중지할 수 없습니다.")
    if selected_id == admin_id and group != "관리자":
        raise ValueError("로그인한 관리자는 관리자 권한을 유지해야 합니다.")
    user_table = qualified(schema_name, "tb_mig_usr")
    auth_table = qualified(schema_name, "tb_mig_usr_auth")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"UPDATE {user_table} SET usr_nm = %s, active_yn = %s, upd_by = %s, upd_dtm = GETDATE() WHERE usr_id = %s", (text(user_name) or selected_id, active, admin_id, selected_id))
            cursor.execute(f"UPDATE {auth_table} SET active_yn = FALSE, upd_by = %s, upd_dtm = GETDATE() WHERE usr_id = %s AND active_yn = TRUE", (admin_id, selected_id))
            for role in GROUP_ROLES[group]:
                cursor.execute(f"INSERT INTO {auth_table} (usr_id, auth_role_cd, prj_cd, sbj_area_cd, active_yn, crt_by, upd_by) VALUES (%s, %s, %s, %s, TRUE, %s, %s)", (selected_id, role, text(project) or None, text(subject) or None, admin_id, admin_id))
        connection.commit()


def reset_password(connect: Callable[[dict[str, Any]], Any], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], selected_id: str, admin_id: str) -> None:
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"UPDATE {qualified(schema_name, 'tb_mig_usr')} SET pwd_hsh_val = %s, pwd_chg_req_yn = TRUE, pwd_chg_dtm = NULL, upd_by = %s, upd_dtm = GETDATE() WHERE usr_id = %s", (password_hash(selected_id), admin_id, selected_id))
        connection.commit()


def render_user_management(values: dict[str, Any], schema_name: str, admin_id: str, query_frame: Callable[..., pd.DataFrame], connect: Callable[[dict[str, Any]], Any], qualified: Callable[[str, str], str]) -> None:
    users = user_frame(query_frame, values, schema_name, qualified)
    st.dataframe(users.rename(columns={"usr_id": "사용자 ID", "usr_nm": "사용자명", "pwd_chg_req_yn": "비밀번호 변경 필요", "active_yn": "사용", "last_login_dtm": "최종 로그인", "pwd_chg_dtm": "비밀번호 변경일시", "crt_dtm": "등록일시", "upd_dtm": "수정일시"}), hide_index=True, height=260)
    mode = st.segmented_control("사용자 관리 업무", ["사용자 등록", "권한·상태 관리"], default="사용자 등록", label_visibility="collapsed")
    if mode == "사용자 등록":
        with st.form("mig_user_create_form"):
            user_id = st.text_input("사용자 ID")
            user_name = st.text_input("사용자명")
            group = st.selectbox("권한 그룹", list(GROUP_ROLES), index=1)
            project = st.text_input("프로젝트코드 범위", placeholder="비우면 전체")
            subject = st.text_input("주제영역코드 범위", placeholder="비우면 전체")
            submitted = st.form_submit_button("사용자 등록", type="primary", icon=":material/person_add:")
        if submitted:
            try:
                save_new_user(connect, values, schema_name, qualified, user_id, user_name, group, project, subject, admin_id)
                st.success("사용자를 등록했습니다. 초기 비밀번호는 사용자 ID와 같으며 첫 로그인에서 변경해야 합니다.", icon=":material/check_circle:")
                st.rerun()
            except Exception as error:
                st.error(f"사용자 등록 실패: {error}", icon=":material/error:")
        return
    if users.empty:
        st.info("등록된 사용자가 없습니다.", icon=":material/info:")
        return
    selected_id = st.selectbox("사용자", users.usr_id.tolist())
    current = users.loc[users.usr_id.eq(selected_id)].iloc[0]
    authorizations = authorization_frame(query_frame, values, schema_name, qualified, selected_id)
    group = group_from_authorizations(authorizations)
    project, subject = first_scope(authorizations)
    with st.form("mig_user_detail_form"):
        user_name = st.text_input("사용자명", value=text(current.usr_nm))
        active = st.toggle("사용", value=bool(current.active_yn))
        group = st.selectbox("권한 그룹", list(GROUP_ROLES), index=list(GROUP_ROLES).index(group))
        project = st.text_input("프로젝트코드 범위", value=project, placeholder="비우면 전체")
        subject = st.text_input("주제영역코드 범위", value=subject, placeholder="비우면 전체")
        saved = st.form_submit_button("권한·상태 저장", type="primary", icon=":material/save:")
    if saved:
        try:
            save_user_detail(connect, values, schema_name, qualified, selected_id, user_name, active, group, project, subject, admin_id)
            st.success("사용자 권한과 상태를 저장했습니다.", icon=":material/check_circle:")
            st.rerun()
        except Exception as error:
            st.error(f"사용자 저장 실패: {error}", icon=":material/error:")
    if st.button("초기 비밀번호로 재설정", icon=":material/restart_alt:"):
        try:
            reset_password(connect, values, schema_name, qualified, selected_id, admin_id)
            st.success("초기 비밀번호를 사용자 ID와 같게 재설정했습니다.", icon=":material/check_circle:")
        except Exception as error:
            st.error(f"비밀번호 초기화 실패: {error}", icon=":material/error:")

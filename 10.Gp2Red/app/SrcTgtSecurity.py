from __future__ import annotations

import hashlib
import hmac
import re
import secrets
from typing import Any

import pandas as pd
import streamlit as st

from SrcTgtRuntime import connect, qualified, query_frame, text


AUTH_TABLES = {"tb_mig_usr", "tb_mig_auth_grp", "tb_mig_menu_auth", "tb_mig_usr_auth"}
PASSWORD_ITERATIONS = 390000
MENU_OPTIONS = [
    ("CONN", "접속정보"), ("SBJ", "주제영역"), ("LAYOUT", "테이블 레이아웃"), ("DDL", "대상 DDL"),
    ("MPG", "SRC·TGT 매핑"), ("DAG", "DAG 생성"), ("MON", "실행 현황"), ("CMP", "테이블 변경 비교"),
    ("VALD", "검증"), ("RUN", "실행 이력"), ("EMR", "EMR"), ("SNAP", "스냅샷 복구"),
    ("ARTF", "산출물 관리"), ("USR", "사용자 관리"), ("AUTH", "권한 관리"), ("INIT", "초기 설정"),
]


def user_id(value: object) -> str:
    candidate = text(value)
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,99}", candidate):
        raise ValueError("사용자 ID는 영문으로 시작하는 영문·숫자·밑줄·점·하이픈 1~100자리여야 합니다.")
    return candidate


def group_id(value: object) -> str:
    candidate = text(value).upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,99}", candidate):
        raise ValueError("권한그룹 ID는 영문으로 시작하는 영문·숫자·밑줄 1~100자리여야 합니다.")
    return candidate


def password_hash(password: str, salt: bytes | None = None, enforce_length: bool = True) -> str:
    if enforce_length and len(password) < 8:
        raise ValueError("비밀번호는 8자리 이상으로 입력하십시오.")
    secret = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), secret, PASSWORD_ITERATIONS)
    return f"PBKDF2_SHA256${PASSWORD_ITERATIONS}${secret.hex()}${digest.hex()}"


def verify_password(password: str, encoded: object) -> bool:
    try:
        algorithm, count, salt_hex, digest_hex = text(encoded).split("$", 3)
        if algorithm != "PBKDF2_SHA256":
            return False
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt_hex), int(count))
        return hmac.compare_digest(digest.hex(), digest_hex)
    except (TypeError, ValueError):
        return False


def auth_ready(values: dict[str, Any], schema_name: str) -> bool:
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = ANY(%s)", (schema_name, list(AUTH_TABLES)))
            return {text(row[0]).lower() for row in cursor.fetchall()} == AUTH_TABLES


def current_user() -> dict[str, Any] | None:
    value = st.session_state.get("mig_authenticated_user")
    return dict(value) if isinstance(value, dict) else None


def clear_login() -> None:
    st.session_state.pop("mig_authenticated_user", None)
    st.session_state.pop("mig_active_menu", None)


def permissions(values: dict[str, Any], schema_name: str, user: str) -> dict[str, dict[str, bool]]:
    query = f'''SELECT M.menu_cd,
                       MAX(CASE WHEN M.read_yn THEN 1 ELSE 0 END) AS read_yn,
                       MAX(CASE WHEN M.save_yn THEN 1 ELSE 0 END) AS save_yn
                  FROM {qualified(schema_name, "tb_mig_usr")} X
                  JOIN {qualified(schema_name, "tb_mig_usr_auth")} U ON U.usr_id = X.usr_id
                  JOIN {qualified(schema_name, "tb_mig_auth_grp")} G ON G.auth_grp_id = U.auth_grp_id AND G.use_yn = TRUE
                  JOIN {qualified(schema_name, "tb_mig_menu_auth")} M ON M.auth_grp_id = U.auth_grp_id
                 WHERE X.usr_id = %s
                   AND X.use_yn = TRUE
                   AND (X.vald_stt_dt IS NULL OR X.vald_stt_dt <= CURRENT_DATE)
                   AND (X.vald_end_dt IS NULL OR X.vald_end_dt >= CURRENT_DATE)
                 GROUP BY M.menu_cd'''
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (user,))
            return {text(row[0]).upper(): {"read": bool(row[1]), "save": bool(row[2])} for row in cursor.fetchall()}


def authenticate(values: dict[str, Any], schema_name: str, login_id: str, password: str) -> dict[str, Any] | None:
    candidate = user_id(login_id)
    query = f'''SELECT usr_id, usr_nm, pwd_hsh, pwd_chg_yn
                  FROM {qualified(schema_name, "tb_mig_usr")}
                 WHERE usr_id = %s
                   AND use_yn = TRUE
                   AND (vald_stt_dt IS NULL OR vald_stt_dt <= CURRENT_DATE)
                   AND (vald_end_dt IS NULL OR vald_end_dt >= CURRENT_DATE)'''
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (candidate,))
            record = cursor.fetchone()
    if record is None or not verify_password(password, record[2]):
        return None
    return {"usr_id": text(record[0]), "usr_nm": text(record[1]), "pwd_chg_yn": bool(record[3]), "bootstrap": False, "permissions": permissions(values, schema_name, text(record[0]))}


def bootstrap_authenticate(login_id: str, password: str) -> dict[str, Any] | None:
    if hmac.compare_digest(text(login_id), "admin") and hmac.compare_digest(password, "admin"):
        return {"usr_id": "admin", "usr_nm": "관리자", "pwd_chg_yn": False, "bootstrap": True, "permissions": {"INIT": {"read": True, "save": True}}}
    return None


def save_login(user: dict[str, Any]) -> None:
    st.session_state["mig_authenticated_user"] = user


def allowed(menu_code: str, action: str = "read") -> bool:
    user = current_user()
    if user is None:
        return False
    permission = user.get("permissions", {}).get(menu_code.upper(), {})
    return bool(permission.get(action, False))


def require_save(menu_code: str) -> None:
    if not allowed(menu_code, "save"):
        raise PermissionError("이 메뉴의 저장 권한이 없습니다.")


def change_password(values: dict[str, Any], schema_name: str, user: str, password: str) -> None:
    encoded = password_hash(password)
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"UPDATE {qualified(schema_name, 'tb_mig_usr')} SET pwd_hsh = %s, pwd_chg_yn = FALSE, upd_dtm = GETDATE() WHERE usr_id = %s", (encoded, user))
        connection.commit()


def render_login(values: dict[str, Any] | None, schema_name: str, metadata_available: bool) -> None:
    left, center, right = st.columns([1.2, 1, 1.2])
    with center:
        st.subheader(":material/login: 로그인")
        with st.form("mig_login_form"):
            login_id = st.text_input("사용자 ID", key="mig_login_id")
            password = st.text_input("비밀번호", type="password", key="mig_login_password")
            submitted = st.form_submit_button("로그인", type="primary", icon=":material/login:", width="stretch")
    if submitted:
        try:
            user = authenticate(values, schema_name, login_id, password) if metadata_available and values is not None else bootstrap_authenticate(login_id, password)
            if user is None:
                st.error("사용자 ID 또는 비밀번호가 올바르지 않습니다.", icon=":material/error:")
            else:
                save_login(user)
                st.rerun()
        except Exception as error:
            st.error(f"로그인 실패: {error}", icon=":material/error:")


def render_password_change(values: dict[str, Any], schema_name: str, user: dict[str, Any]) -> None:
    left, center, right = st.columns([1.2, 1, 1.2])
    with center:
        st.subheader(":material/password: 비밀번호 변경")
        with st.form("mig_password_change_form"):
            password = st.text_input("새 비밀번호", type="password")
            confirmed = st.text_input("새 비밀번호 확인", type="password")
            submitted = st.form_submit_button("비밀번호 변경", type="primary", icon=":material/save:", width="stretch")
    if submitted:
        try:
            if password != confirmed:
                raise ValueError("새 비밀번호와 확인 값이 다릅니다.")
            change_password(values, schema_name, text(user["usr_id"]), password)
            refreshed = authenticate(values, schema_name, text(user["usr_id"]), password)
            if refreshed is None:
                raise ValueError("변경한 비밀번호를 확인할 수 없습니다.")
            save_login(refreshed)
            st.success("비밀번호를 변경했습니다.", icon=":material/check_circle:")
            st.rerun()
        except Exception as error:
            st.error(f"비밀번호 변경 실패: {error}", icon=":material/error:")


def group_frame(values: dict[str, Any], schema_name: str) -> pd.DataFrame:
    return query_frame(values, f"SELECT auth_grp_id, auth_grp_nm, use_yn, crt_dtm, upd_dtm FROM {qualified(schema_name, 'tb_mig_auth_grp')} ORDER BY auth_grp_id")


def user_frame(values: dict[str, Any], schema_name: str) -> pd.DataFrame:
    query = f'''SELECT U.usr_id, U.usr_nm, U.use_yn, U.pwd_chg_yn, U.vald_stt_dt, U.vald_end_dt,
                       LISTAGG(A.auth_grp_id, ', ') WITHIN GROUP (ORDER BY A.auth_grp_id) AS auth_grp_ids,
                       U.upd_dtm
                  FROM {qualified(schema_name, "tb_mig_usr")} U
                  LEFT JOIN {qualified(schema_name, "tb_mig_usr_auth")} A ON A.usr_id = U.usr_id
                 GROUP BY U.usr_id, U.usr_nm, U.use_yn, U.pwd_chg_yn, U.vald_stt_dt, U.vald_end_dt, U.upd_dtm
                 ORDER BY U.usr_id'''
    return query_frame(values, query)


def save_user(values: dict[str, Any], schema_name: str, record: dict[str, Any], groups: list[str], is_new: bool) -> None:
    candidate = user_id(record["usr_id"])
    name = text(record["usr_nm"])
    if not name or len(name) > 200:
        raise ValueError("사용자명은 1~200자로 입력하십시오.")
    normalized_groups = sorted({group_id(value) for value in groups})
    table_name = qualified(schema_name, "tb_mig_usr")
    mapping_table = qualified(schema_name, "tb_mig_usr_auth")
    group_table = qualified(schema_name, "tb_mig_auth_grp")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM {table_name} WHERE usr_id = %s", (candidate,))
            exists = cursor.fetchone() is not None
            if is_new and exists:
                raise ValueError("이미 등록된 사용자 ID입니다.")
            if not is_new and not exists:
                raise ValueError("수정할 사용자가 없습니다.")
            for value in normalized_groups:
                cursor.execute(f"SELECT 1 FROM {group_table} WHERE auth_grp_id = %s AND use_yn = TRUE", (value,))
                if cursor.fetchone() is None:
                    raise ValueError(f"사용 가능한 권한그룹이 아닙니다: {value}")
            if is_new:
                cursor.execute(f"INSERT INTO {table_name} (usr_id, usr_nm, pwd_hsh, pwd_chg_yn, use_yn, vald_stt_dt, vald_end_dt) VALUES (%s, %s, %s, TRUE, %s, %s, %s)", (candidate, name, password_hash(candidate, enforce_length=False), bool(record["use_yn"]), record.get("vald_stt_dt"), record.get("vald_end_dt")))
            else:
                cursor.execute(f"UPDATE {table_name} SET usr_nm = %s, use_yn = %s, vald_stt_dt = %s, vald_end_dt = %s, upd_dtm = GETDATE() WHERE usr_id = %s", (name, bool(record["use_yn"]), record.get("vald_stt_dt"), record.get("vald_end_dt"), candidate))
            cursor.execute(f"DELETE FROM {mapping_table} WHERE usr_id = %s", (candidate,))
            for value in normalized_groups:
                cursor.execute(f"INSERT INTO {mapping_table} (usr_id, auth_grp_id) VALUES (%s, %s)", (candidate, value))
        connection.commit()


def permission_frame(values: dict[str, Any], schema_name: str, selected_group: str) -> pd.DataFrame:
    query = f'''SELECT M.menu_cd, M.read_yn, M.save_yn
                  FROM {qualified(schema_name, "tb_mig_menu_auth")} M
                 WHERE M.auth_grp_id = %s'''
    existing = query_frame(values, query, (selected_group,))
    result = pd.DataFrame(MENU_OPTIONS, columns=["menu_cd", "메뉴"])
    result = result.merge(existing, how="left", on="menu_cd")
    result["read_yn"] = result.read_yn.fillna(False).astype(bool)
    result["save_yn"] = result.save_yn.fillna(False).astype(bool)
    return result


def save_group(values: dict[str, Any], schema_name: str, record: dict[str, Any], permissions_frame: pd.DataFrame, is_new: bool) -> None:
    candidate = group_id(record["auth_grp_id"])
    name = text(record["auth_grp_nm"])
    if not name or len(name) > 200:
        raise ValueError("권한그룹명은 1~200자로 입력하십시오.")
    group_table = qualified(schema_name, "tb_mig_auth_grp")
    menu_table = qualified(schema_name, "tb_mig_menu_auth")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM {group_table} WHERE auth_grp_id = %s", (candidate,))
            exists = cursor.fetchone() is not None
            if is_new and exists:
                raise ValueError("이미 등록된 권한그룹 ID입니다.")
            if not is_new and not exists:
                raise ValueError("수정할 권한그룹이 없습니다.")
            if is_new:
                cursor.execute(f"INSERT INTO {group_table} (auth_grp_id, auth_grp_nm, use_yn) VALUES (%s, %s, %s)", (candidate, name, bool(record["use_yn"])))
            else:
                cursor.execute(f"UPDATE {group_table} SET auth_grp_nm = %s, use_yn = %s, upd_dtm = GETDATE() WHERE auth_grp_id = %s", (name, bool(record["use_yn"]), candidate))
            cursor.execute(f"DELETE FROM {menu_table} WHERE auth_grp_id = %s", (candidate,))
            for item in permissions_frame.to_dict("records"):
                read_allowed = bool(item.get("read_yn", False))
                save_allowed = read_allowed and bool(item.get("save_yn", False))
                cursor.execute(f"INSERT INTO {menu_table} (auth_grp_id, menu_cd, read_yn, save_yn) VALUES (%s, %s, %s, %s)", (candidate, text(item["menu_cd"]), read_allowed, save_allowed))
        connection.commit()

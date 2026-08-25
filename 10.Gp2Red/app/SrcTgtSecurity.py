from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import hmac
import secrets
import sys
from typing import Any

import pandas as pd
import streamlit as st

from SrcTgtSetup import configured_schema

try:
    import psycopg
except ImportError:
    psycopg = None


AUTH_USER_KEY = "mig_authenticated_user_id"
AUTH_CHANGE_KEY = "mig_password_change_required"
PASSWORD_MIN_LENGTH = 10


@dataclass(frozen=True)
class AccessContext:
    user_id: str
    values: dict[str, Any]
    schema_name: str
    authorizations: pd.DataFrame


def text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def identifier(value: str) -> str:
    name = text(value)
    if not name or "\x00" in name or len(name.encode("utf-8")) > 127:
        raise ValueError("메타데이터 식별자 형식이 올바르지 않습니다.")
    return '"' + name.replace('"', '""') + '"'


def qualified(schema_name: str, table_name: str) -> str:
    return f"{identifier(schema_name)}.{identifier(table_name)}"


def metadata_settings() -> tuple[dict[str, Any], str]:
    settings = dict(st.secrets.get("migration_metadata", {}))
    section = text(settings.get("connection_section")) or "redshift_sql"
    schema_name = text(settings.get("schema")).lower() or configured_schema()
    if not schema_name:
        raise ValueError("초기 설정에서 메타데이터 스키마를 먼저 생성하십시오.")
    if section not in st.secrets:
        raise ValueError("이관 메타데이터 연결 설정이 없습니다.")
    values = dict(st.secrets[section])
    required = ("host", "port", "database", "user", "password")
    if [key for key in required if not text(values.get(key))]:
        raise ValueError("이관 메타데이터 연결 필수 항목이 없습니다.")
    return values, schema_name


def public_monitor_context() -> AccessContext:
    settings = dict(st.secrets.get("migration_monitor", {}))
    section = text(settings.get("connection_section"))
    if not section:
        values, schema_name = metadata_settings()
    else:
        schema_name = text(settings.get("schema")) or "mig_meta"
        if section not in st.secrets:
            raise ValueError("고객 현황 연결 설정이 없습니다.")
        values = dict(st.secrets[section])
        required = ("host", "port", "database", "user", "password")
        if [key for key in required if not text(values.get(key))]:
            raise ValueError("고객 현황 연결 필수 항목이 없습니다.")
    return AccessContext(user_id="PUBLIC", values=values, schema_name=schema_name, authorizations=pd.DataFrame(columns=["auth_role_cd", "prj_cd", "sbj_area_cd"]))


def connect(values: dict[str, Any]) -> Any:
    if psycopg is None:
        raise RuntimeError(f"psycopg가 현재 실행 Python에 설치되지 않았습니다: {sys.executable}")
    arguments: dict[str, Any] = {
        "host": text(values["host"]), "port": int(values["port"]), "dbname": text(values["database"]),
        "user": text(values["user"]), "password": text(values["password"]),
        "connect_timeout": int(values.get("connect_timeout", 15)),
    }
    if text(values.get("sslmode")):
        arguments["sslmode"] = text(values["sslmode"])
    return psycopg.connect(**arguments)


def query_frame(values: dict[str, Any], query: str, parameters: tuple[Any, ...] = ()) -> pd.DataFrame:
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
            return pd.DataFrame(cursor.fetchall(), columns=[column.name for column in cursor.description])


def encode_value(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def decode_value(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def password_hash(password: str, salt: bytes | None = None) -> str:
    actual_salt = salt or secrets.token_bytes(16)
    derived = hashlib.scrypt(password.encode("utf-8"), salt=actual_salt, n=32768, r=8, p=1, dklen=32, maxmem=67108864)
    return f"scrypt$32768$8$1${encode_value(actual_salt)}${encode_value(derived)}"


def password_matches(password: str, stored: object) -> bool:
    try:
        algorithm, n_value, r_value, p_value, salt_value, hash_value = text(stored).split("$")
        if algorithm != "scrypt":
            return False
        derived = hashlib.scrypt(password.encode("utf-8"), salt=decode_value(salt_value), n=int(n_value), r=int(r_value), p=int(p_value), dklen=len(decode_value(hash_value)), maxmem=67108864)
        return hmac.compare_digest(encode_value(derived), hash_value)
    except (ValueError, TypeError):
        return False


def validate_new_password(password: str, confirmation: str) -> None:
    if password != confirmation:
        raise ValueError("새 비밀번호와 확인 값이 일치하지 않습니다.")
    if len(password) < PASSWORD_MIN_LENGTH:
        raise ValueError(f"새 비밀번호는 {PASSWORD_MIN_LENGTH}자 이상이어야 합니다.")


def account_frame(values: dict[str, Any], schema_name: str, user_id: str) -> pd.DataFrame:
    query = f"SELECT usr_id, usr_nm, pwd_hsh_val, pwd_chg_req_yn, active_yn FROM {qualified(schema_name, 'tb_mig_usr')} WHERE usr_id = %s"
    return query_frame(values, query, (user_id,))


def set_login_state(user_id: str, password_change_required: bool) -> None:
    st.session_state[AUTH_USER_KEY] = user_id
    st.session_state[AUTH_CHANGE_KEY] = password_change_required


def clear_login_state() -> None:
    st.session_state.pop(AUTH_USER_KEY, None)
    st.session_state.pop(AUTH_CHANGE_KEY, None)


def render_login() -> None:
    st.title("🔐 이관 관리 로그인")
    with st.container(border=True):
        with st.form("mig_local_login_form"):
            user_id = st.text_input("사용자 ID", key="mig_login_user_id", autocomplete="username")
            password = st.text_input("비밀번호", type="password", key="mig_login_password", autocomplete="current-password")
            submitted = st.form_submit_button("로그인", type="primary", icon=":material/login:")
        if submitted:
            try:
                values, schema_name = metadata_settings()
                account = account_frame(values, schema_name, text(user_id))
                if account.empty or not bool(account.iloc[0].active_yn) or not password_matches(password, account.iloc[0].pwd_hsh_val):
                    raise ValueError("사용자 ID 또는 비밀번호가 올바르지 않습니다.")
                with connect(values) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(f"UPDATE {qualified(schema_name, 'tb_mig_usr')} SET last_login_dtm = GETDATE(), upd_by = %s, upd_dtm = GETDATE() WHERE usr_id = %s", (text(user_id), text(user_id)))
                    connection.commit()
                set_login_state(text(account.iloc[0].usr_id), bool(account.iloc[0].pwd_chg_req_yn))
                st.rerun()
            except ValueError as error:
                st.error(str(error), icon=":material/lock:")
            except Exception:
                st.error("로그인 정보를 확인할 수 없습니다.", icon=":material/lock:")
    st.caption("⚙️ Created by ♡홍율파파")


def render_password_change(values: dict[str, Any], schema_name: str, user_id: str) -> None:
    st.title("🔐 초기 비밀번호 변경")
    st.info("초기 비밀번호는 변경 후에만 관리 화면을 사용할 수 있습니다.", icon=":material/key:")
    with st.container(border=True):
        with st.form("mig_password_change_form"):
            current_password = st.text_input("현재 비밀번호", type="password", key="mig_current_password", autocomplete="current-password")
            new_password = st.text_input(f"새 비밀번호 ({PASSWORD_MIN_LENGTH}자 이상)", type="password", key="mig_new_password", autocomplete="new-password")
            confirmation = st.text_input("새 비밀번호 확인", type="password", key="mig_new_password_confirmation", autocomplete="new-password")
            submitted = st.form_submit_button("비밀번호 변경", type="primary", icon=":material/key:")
        if submitted:
            try:
                account = account_frame(values, schema_name, user_id)
                if account.empty or not bool(account.iloc[0].active_yn) or not password_matches(current_password, account.iloc[0].pwd_hsh_val):
                    raise ValueError("현재 비밀번호가 올바르지 않습니다.")
                validate_new_password(new_password, confirmation)
                if password_matches(new_password, account.iloc[0].pwd_hsh_val):
                    raise ValueError("현재 비밀번호와 다른 비밀번호를 입력하십시오.")
                with connect(values) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(f"UPDATE {qualified(schema_name, 'tb_mig_usr')} SET pwd_hsh_val = %s, pwd_chg_req_yn = FALSE, pwd_chg_dtm = GETDATE(), upd_by = %s, upd_dtm = GETDATE() WHERE usr_id = %s", (password_hash(new_password), user_id, user_id))
                    connection.commit()
                set_login_state(user_id, False)
                st.rerun()
            except ValueError as error:
                st.error(str(error), icon=":material/error:")
            except Exception:
                st.error("비밀번호를 변경할 수 없습니다.", icon=":material/error:")
    st.caption("⚙️ Created by ♡홍율파파")


def require_access() -> AccessContext:
    user_id = text(st.session_state.get(AUTH_USER_KEY))
    if not user_id:
        render_login()
        st.stop()
    try:
        values, schema_name = metadata_settings()
        account = account_frame(values, schema_name, user_id)
        if account.empty or not bool(account.iloc[0].active_yn):
            clear_login_state()
            st.error("사용할 수 없는 계정입니다.", icon=":material/lock:")
            render_login()
            st.stop()
        if bool(account.iloc[0].pwd_chg_req_yn):
            render_password_change(values, schema_name, user_id)
            st.stop()
        query = f"SELECT auth_role_cd, prj_cd, sbj_area_cd FROM {qualified(schema_name, 'tb_mig_usr_auth')} WHERE usr_id = %s AND active_yn = TRUE"
        authorizations = query_frame(values, query, (user_id,))
    except Exception:
        clear_login_state()
        st.error("이관 권한을 확인할 수 없습니다.", icon=":material/lock:")
        st.stop()
    if authorizations.empty:
        st.error("이관 관리 권한이 없습니다.", icon=":material/lock:")
        st.stop()
    return AccessContext(user_id=user_id, values=values, schema_name=schema_name, authorizations=authorizations)


def logout() -> None:
    clear_login_state()
    st.rerun()


def allowed(authorizations: pd.DataFrame, role: str, project: object = None, subject_area: object = None) -> bool:
    project_code, area_code = text(project), text(subject_area)
    for row in authorizations.itertuples(index=False):
        if text(row.auth_role_cd).upper() not in {"ADMIN", role}:
            continue
        if text(row.prj_cd) and text(row.prj_cd) != project_code:
            continue
        if text(row.sbj_area_cd) and text(row.sbj_area_cd) != area_code:
            continue
        return True
    return False

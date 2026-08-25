from __future__ import annotations

import re
import sys
from typing import Any

import pandas as pd
import streamlit as st

from SrcTgtValidation import render_validation
from SrcTgtArtifact import render_artifacts
from SrcTgtDagGenerator import render_dag_generator
from SrcTgtMapping import render_mapping_workspace
from SrcTgtSecurity import require_access
from SrcTgtUser import render_user_management

try:
    import psycopg
except ImportError:
    psycopg = None


TABLE_COLUMNS = [
    "mpg_id", "prj_cd", "sbj_area_cd", "src_conn_id", "src_sch_nm", "src_tbl_nm",
    "tgt_conn_id", "tgt_sch_nm", "tgt_tbl_nm", "tgt_dist_style", "tgt_dist_key_col",
    "tgt_sort_style", "tgt_sort_cols", "tgt_encd_auto_yn", "load_mthd_cd",
    "trnsf_pfl_cd", "s3_stg_path", "s3_file_fmt_cd",
    "ddl_aprv_sts_cd", "mpg_sts_cd", "meta_ver_no",
]

DISPLAY_COLUMNS = {
    "mpg_id": "매핑 ID", "prj_cd": "프로젝트", "sbj_area_cd": "주제영역",
    "src_conn_id": "원천 접속", "src_sch_nm": "원천 스키마", "src_tbl_nm": "원천 테이블",
    "tgt_conn_id": "대상 접속", "tgt_sch_nm": "대상 스키마", "tgt_tbl_nm": "대상 테이블",
    "tgt_dist_style": "분산 방식", "tgt_dist_key_col": "분산키", "tgt_sort_style": "정렬 방식",
    "tgt_sort_cols": "정렬키", "tgt_encd_auto_yn": "자동 압축", "load_mthd_cd": "적재 방식",
    "trnsf_pfl_cd": "실행 프로파일", "s3_stg_path": "S3 경로", "s3_file_fmt_cd": "S3 파일 형식",
    "ddl_aprv_sts_cd": "DDL 승인",
    "mpg_sts_cd": "매핑 승인", "meta_ver_no": "버전",
}


def text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def identifier(value: str) -> str:
    name = text(value)
    if not name or "\x00" in name or len(name.encode("utf-8")) > 127:
        raise ValueError(f"식별자 형식이 올바르지 않습니다: {value}")
    return '"' + name.replace('"', '""') + '"'


def qualified(schema_name: str, table_name: str) -> str:
    return f"{identifier(schema_name)}.{identifier(table_name)}"


def metadata_settings() -> tuple[dict[str, Any], str]:
    settings = dict(st.secrets.get("migration_metadata", {}))
    section = text(settings.get("connection_section")) or "redshift_sql"
    schema_name = text(settings.get("schema")) or "mig_meta"
    if section not in st.secrets:
        raise ValueError(f".streamlit/secrets.toml에 [{section}] 설정이 없습니다.")
    values = dict(st.secrets[section])
    required = ("host", "port", "database", "user", "password")
    missing = [key for key in required if not text(values.get(key))]
    if missing:
        raise ValueError(f"[{section}] 필수 항목이 없습니다: {', '.join(missing)}")
    return values, schema_name


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


def execute(values: dict[str, Any], query: str, parameters: tuple[Any, ...]) -> None:
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
        connection.commit()


def user_authorizations(values: dict[str, Any], schema_name: str, user_id: str) -> pd.DataFrame:
    query = f"SELECT auth_role_cd, prj_cd, sbj_area_cd FROM {qualified(schema_name, 'tb_mig_usr_auth')} WHERE usr_id = %s AND active_yn = TRUE"
    return query_frame(values, query, (user_id,))


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


def visible_maps(maps: pd.DataFrame, authorizations: pd.DataFrame) -> pd.DataFrame:
    if maps.empty or authorizations.empty:
        return maps.iloc[0:0]
    if authorizations.auth_role_cd.map(text).str.upper().eq("ADMIN").any():
        return maps
    visible = pd.Series(False, index=maps.index)
    for row in authorizations.itertuples(index=False):
        project_code, area_code = text(row.prj_cd), text(row.sbj_area_cd)
        condition = pd.Series(True, index=maps.index)
        if project_code:
            condition &= maps.prj_cd.eq(project_code)
        if area_code:
            condition &= maps.sbj_area_cd.eq(area_code)
        visible |= condition
    return maps.loc[visible].copy()


def subject_areas(values: dict[str, Any], schema_name: str) -> pd.DataFrame:
    query = f"""
        SELECT S.sbj_area_cd, S.sbj_area_nm, S.up_sbj_area_cd, P.sbj_area_nm AS up_sbj_area_nm, S.disp_ord, S.active_yn,
               D.dag_id, D.dflt_parl_cnt, D.max_parl_cnt
          FROM {qualified(schema_name, 'tb_mig_sbj_area')} S
          LEFT JOIN {qualified(schema_name, 'tb_mig_sbj_area')} P
            ON P.sbj_area_cd = S.up_sbj_area_cd
          LEFT JOIN {qualified(schema_name, 'tb_mig_sbj_dag_mpg')} D
            ON D.sbj_area_cd = S.sbj_area_cd
         ORDER BY S.disp_ord, S.sbj_area_cd
    """
    return query_frame(values, query)


def subject_code(value: object, label: str) -> str:
    code = text(value).upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,7}", code):
        raise ValueError(f"{label}는 영문으로 시작하는 영문·숫자·밑줄 1~8자리여야 합니다.")
    return code


def save_subject_area(values: dict[str, Any], schema_name: str, area: dict[str, Any], user_id: str) -> None:
    if int(area["default_parallel"]) < 1 or int(area["maximum_parallel"]) < int(area["default_parallel"]):
        raise ValueError("기본 병렬도는 1 이상이고 최대 병렬도 이하여야 합니다.")
    old_code = subject_code(area["old_code"], "기존 주제영역 코드")
    code = subject_code(area["code"], "주제영역 코드")
    parent = "" if area["is_parent"] else subject_code(area["parent"], "상위 주제영역 코드")
    if code == parent:
        raise ValueError("주제영역 코드와 상위 주제영역 코드는 같을 수 없습니다.")
    table_name = qualified(schema_name, "tb_mig_sbj_area")
    dag_table = qualified(schema_name, "tb_mig_sbj_dag_mpg")
    mapping_table = qualified(schema_name, "tb_mig_tbl_mpg")
    authorization_table = qualified(schema_name, "tb_mig_usr_auth")
    duplicate_sql = f"SELECT 1 FROM {table_name} WHERE sbj_area_cd = %s AND sbj_area_cd <> %s"
    parent_sql = f"SELECT 1 FROM {table_name} WHERE sbj_area_cd = %s AND up_sbj_area_cd IS NULL"
    update_sql = f"UPDATE {table_name} SET sbj_area_cd = %s, sbj_area_nm = %s, up_sbj_area_cd = %s, disp_ord = %s, active_yn = %s, upd_by = %s, upd_dtm = GETDATE() WHERE sbj_area_cd = %s"
    dag_sql = f"UPDATE {dag_table} SET dag_id = %s, dflt_parl_cnt = %s, max_parl_cnt = %s, upd_by = %s, upd_dtm = GETDATE() WHERE sbj_area_cd = %s"
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(duplicate_sql, (code, old_code))
            if cursor.fetchone() is not None:
                raise ValueError("이미 사용 중인 주제영역 코드입니다.")
            if parent:
                cursor.execute(parent_sql, (parent,))
                if cursor.fetchone() is None:
                    raise ValueError("사용 가능한 상위 주제영역 코드를 선택하십시오.")
            cursor.execute(update_sql, (code, text(area["name"]) or None, parent or None, int(area["display_order"]), area["active"], user_id, old_code))
            if cursor.rowcount != 1:
                raise ValueError("수정할 주제영역을 찾을 수 없습니다.")
            if code != old_code:
                cursor.execute(f"UPDATE {table_name} SET up_sbj_area_cd = %s, upd_by = %s, upd_dtm = GETDATE() WHERE up_sbj_area_cd = %s", (code, user_id, old_code))
                cursor.execute(f"UPDATE {dag_table} SET sbj_area_cd = %s, upd_by = %s, upd_dtm = GETDATE() WHERE sbj_area_cd = %s", (code, user_id, old_code))
                cursor.execute(f"UPDATE {mapping_table} SET sbj_area_cd = %s, upd_by = %s, upd_dtm = GETDATE() WHERE sbj_area_cd = %s", (code, user_id, old_code))
                cursor.execute(f"UPDATE {authorization_table} SET sbj_area_cd = %s, upd_by = %s, upd_dtm = GETDATE() WHERE sbj_area_cd = %s", (code, user_id, old_code))
            if parent:
                cursor.execute(dag_sql, (f"mig_{code.lower()}", area["default_parallel"], area["maximum_parallel"], user_id, code))
        connection.commit()


def table_maps(values: dict[str, Any], schema_name: str) -> pd.DataFrame:
    columns = ", ".join(identifier(column) for column in TABLE_COLUMNS)
    query = f"SELECT {columns} FROM {qualified(schema_name, 'tb_mig_tbl_mpg')} WHERE active_yn = TRUE ORDER BY prj_cd, sbj_area_cd, mpg_id"
    return query_frame(values, query)


def column_maps(values: dict[str, Any], schema_name: str, mapping_id: int) -> pd.DataFrame:
    query = f"""
        SELECT col_ord, src_col_no, src_col_nm, src_data_type, src_null_yn, src_key_role_cd,
               tgt_col_no, tgt_col_nm, tgt_data_type, tgt_null_yn, tgt_key_role_cd,
               trnsf_expr, dflt_expr, sum_vald_yn, hsh_vald_yn
          FROM {qualified(schema_name, 'tb_mig_col_mpg')}
         WHERE mpg_id = %s AND active_yn = TRUE
         ORDER BY col_ord
    """
    return query_frame(values, query, (mapping_id,))


def run_logs(values: dict[str, Any], schema_name: str) -> pd.DataFrame:
    query = f"""
        SELECT wrk_dt, dag_nm, dag_run_id, task_nm, mpg_id, meta_ver_no, sql_file_path, log_file_path, wrk_cnd_val,
               wrk_step_cd, wrk_sts_cd, src_row_cnt, tgt_row_cnt, src_size_byte, tgt_size_byte, wrk_stt_dtm, wrk_end_dtm,
               wrk_elps_sec, wrk_msg
          FROM {qualified(schema_name, 'tb_mig_run_log')}
         ORDER BY run_hist_id DESC
         LIMIT 500
    """
    return query_frame(values, query)


def type_definition(value: object) -> str:
    candidate = text(value)
    if not candidate or not re.fullmatch(r"[A-Za-z0-9_(), ]+", candidate):
        raise ValueError(f"대상 데이터 타입 형식이 올바르지 않습니다: {candidate}")
    return candidate.upper()


def ddl_for(table: pd.Series, columns: pd.DataFrame) -> str:
    if columns.empty:
        raise ValueError("DDL을 생성할 컬럼 매핑이 없습니다.")
    definitions = []
    for row in columns.itertuples(index=False):
        nullable = "" if bool(row.tgt_null_yn) else " NOT NULL"
        default = f" DEFAULT {text(row.dflt_expr)}" if text(row.dflt_expr) else ""
        definitions.append(f"    {identifier(text(row.tgt_col_nm))} {type_definition(row.tgt_data_type)}{default}{nullable}")
    diststyle = text(table.tgt_dist_style).upper() or "AUTO"
    if diststyle not in {"AUTO", "EVEN", "KEY", "ALL"}:
        raise ValueError("대상 분산 방식을 확인하십시오.")
    clauses = [f"DISTSTYLE {diststyle}"]
    if diststyle == "KEY":
        if not text(table.tgt_dist_key_col):
            raise ValueError("분산 방식 KEY에는 대상 분산키가 필요합니다.")
        clauses.append(f"DISTKEY ({identifier(text(table.tgt_dist_key_col))})")
    sortstyle, sortkeys = text(table.tgt_sort_style).upper() or "AUTO", [text(value) for value in text(table.tgt_sort_cols).split(",") if text(value)]
    if sortstyle == "AUTO":
        clauses.append("SORTKEY AUTO")
    elif sortstyle in {"COMPOUND", "INTERLEAVED"} and sortkeys:
        clauses.append(f"{sortstyle} SORTKEY ({', '.join(identifier(value) for value in sortkeys)})")
    elif sortstyle not in {"", "NONE"}:
        raise ValueError("대상 정렬 방식을 확인하십시오.")
    if bool(table.tgt_encd_auto_yn):
        clauses.append("ENCODE AUTO")
    return f"CREATE TABLE IF NOT EXISTS {qualified(text(table.tgt_sch_nm), text(table.tgt_tbl_nm))} (\n{',\n'.join(definitions)}\n)\n{'\n'.join(clauses)};"


def save_ddl(values: dict[str, Any], schema_name: str, mapping_id: int, ddl: str, user_id: str) -> None:
    query = f"UPDATE {qualified(schema_name, 'tb_mig_tbl_mpg')} SET tgt_ddl_sql = %s, ddl_aprv_sts_cd = 'DRAFT', meta_ver_no = meta_ver_no + 1, upd_by = %s, upd_dtm = GETDATE() WHERE mpg_id = %s"
    execute(values, query, (ddl, user_id, mapping_id))


def save_approval(values: dict[str, Any], schema_name: str, mapping_id: int, mapping_status: str, ddl_status: str, user_id: str) -> None:
    query = f"""UPDATE {qualified(schema_name, 'tb_mig_tbl_mpg')}
                   SET mpg_sts_cd = %s, ddl_aprv_sts_cd = %s, meta_ver_no = meta_ver_no + 1,
                       aprv_by = CASE WHEN %s = 'APPROVED' AND %s = 'APPROVED' THEN %s ELSE aprv_by END,
                       aprv_dtm = CASE WHEN %s = 'APPROVED' AND %s = 'APPROVED' THEN GETDATE() ELSE aprv_dtm END,
                       upd_by = %s, upd_dtm = GETDATE()
                 WHERE mpg_id = %s"""
    execute(values, query, (mapping_status, ddl_status, mapping_status, ddl_status, user_id, mapping_status, ddl_status, user_id, mapping_id))


def save_physical_design(values: dict[str, Any], schema_name: str, mapping_id: int, design: dict[str, object], user_id: str) -> None:
    query = f'''UPDATE {qualified(schema_name, "tb_mig_tbl_mpg")}
                   SET tgt_dist_style = %s, tgt_dist_key_col = %s, tgt_sort_style = %s, tgt_sort_cols = %s, tgt_encd_auto_yn = %s,
                       meta_ver_no = meta_ver_no + 1, upd_by = %s, upd_dtm = GETDATE()
                 WHERE mpg_id = %s'''
    execute(values, query, (design["tgt_dist_style"], design["tgt_dist_key_col"] or None, design["tgt_sort_style"], design["tgt_sort_cols"] or None, design["tgt_encd_auto_yn"], user_id, mapping_id))


def mapping_label(maps: pd.DataFrame, mapping_id: int) -> str:
    row = maps.loc[maps.mpg_id.eq(mapping_id)].iloc[0]
    return f"{int(mapping_id)} · {row.src_sch_nm}.{row.src_tbl_nm} → {row.tgt_sch_nm}.{row.tgt_tbl_nm}"


def physical_designs(maps: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for row in maps.itertuples(index=False):
        target_dist_style, target_sort_style = text(row.tgt_dist_style).upper(), text(row.tgt_sort_style).upper()
        rows.append({
            "프로젝트": text(row.prj_cd), "주제영역": text(row.sbj_area_cd), "원천 테이블": f"{text(row.src_sch_nm)}.{text(row.src_tbl_nm)}",
            "대상 테이블": f"{text(row.tgt_sch_nm)}.{text(row.tgt_tbl_nm)}", "대상 분산 방식": target_dist_style, "대상 분산키": text(row.tgt_dist_key_col), "대상 정렬 방식": target_sort_style, "대상 정렬키": text(row.tgt_sort_cols), "대상 자동 압축": False if pd.isna(row.tgt_encd_auto_yn) else bool(row.tgt_encd_auto_yn),
        })
    return pd.DataFrame(rows)


access = require_access()
user_id = access.user_id
target = access.values
metadata_schema = access.schema_name
authorizations = access.authorizations
try:
    maps = visible_maps(table_maps(target, metadata_schema), authorizations)
except Exception as error:
    st.error(f"메타데이터 조회 실패: {error}", icon=":material/error:")
    st.stop()

st.title("🧭 이관 관리")
st.caption("⚙️ Created by ♡홍율파파")
cards = st.columns(4)
cards[0].metric("🔗 매핑", f"{len(maps):,}")
cards[1].metric("🧾 DDL 참조", f"{len(maps):,}")
cards[2].metric("✅ 매핑 승인", f"{int(maps.mpg_sts_cd.eq('APPROVED').sum()):,}" if not maps.empty else "0")
cards[3].metric("⚡ 실행 매핑", f"{len(maps):,}")

view_options = ["🗂️ 주제영역", "⚙️ DAG 생성", "🔗 매핑", "🧱 물리설계", "🧾 DDL", "✅ 검증", "📋 실행 이력", "📦 산출물"]
if allowed(authorizations, "ADMIN"):
    view_options.append("👤 사용자")
view = st.segmented_control("업무", view_options, default="🗂️ 주제영역", label_visibility="collapsed")

if view == "🗂️ 주제영역":
    st.subheader("🗂️ 주제영역")
    if st.session_state.pop("subject_area_code_renamed", False):
        st.warning("코드 변경 후에는 새 DAG 파일을 생성하고 기존 Airflow DAG 파일을 별도로 정리하십시오.", icon=":material/warning:")
    areas = subject_areas(target, metadata_schema)
    st.dataframe(areas.rename(columns={"sbj_area_cd": "주제영역 코드", "sbj_area_nm": "주제영역명", "up_sbj_area_cd": "상위 주제영역 코드", "up_sbj_area_nm": "상위 주제영역명", "disp_ord": "순서", "active_yn": "사용", "dag_id": "DAG", "dflt_parl_cnt": "기본 병렬", "max_parl_cnt": "최대 병렬"}), hide_index=True)
    if allowed(authorizations, "EDIT"):
        selected_area = st.selectbox("주제영역", areas.sbj_area_cd.tolist())
        current = areas.loc[areas.sbj_area_cd.eq(selected_area)].iloc[0]
        is_parent = not text(current.up_sbj_area_cd)
        parent_codes = areas.loc[areas.up_sbj_area_cd.map(text).eq(""), "sbj_area_cd"].tolist()
        with st.form("subject_area_form"):
            code = st.text_input("주제영역 코드", value=text(current.sbj_area_cd))
            name = st.text_input("주제영역명", value=text(current.sbj_area_nm))
            if is_parent:
                parent = ""
                st.text_input("상위 주제영역 코드", value="", disabled=True)
            else:
                parent = st.selectbox("상위 주제영역 코드", parent_codes, index=parent_codes.index(text(current.up_sbj_area_cd)))
            display_order = st.number_input("표시 순서", min_value=0, step=1, value=0 if pd.isna(current.disp_ord) else int(current.disp_ord))
            active = st.toggle("사용", value=bool(current.active_yn))
            default_parallel = st.number_input("기본 병렬", min_value=1, step=1, value=1 if pd.isna(current.dflt_parl_cnt) else int(current.dflt_parl_cnt), disabled=is_parent)
            maximum_parallel = st.number_input("최대 병렬", min_value=1, step=1, value=1 if pd.isna(current.max_parl_cnt) else int(current.max_parl_cnt), disabled=is_parent)
            saved = st.form_submit_button("저장", type="primary")
        if saved:
            try:
                if maximum_parallel < default_parallel:
                    raise ValueError("최대 병렬은 기본 병렬 이상이어야 합니다.")
                save_subject_area(target, metadata_schema, {"old_code": text(current.sbj_area_cd), "code": text(code), "name": text(name), "parent": text(parent), "display_order": int(display_order), "active": active, "default_parallel": int(default_parallel), "maximum_parallel": int(maximum_parallel), "is_parent": is_parent}, user_id)
                if text(code).upper() != text(current.sbj_area_cd).upper():
                    st.session_state.subject_area_code_renamed = True
                st.rerun()
            except Exception as error:
                st.error(str(error), icon=":material/error:")

elif view == "⚙️ DAG 생성":
    st.subheader("⚙️ DAG 생성")
    render_dag_generator(subject_areas(target, metadata_schema), allowed(authorizations, "EDIT"))

elif view == "✅ 검증":
    st.subheader("✅ 검증")
    if not allowed(authorizations, "EXEC"):
        st.error("검증 실행 권한이 없습니다.", icon=":material/lock:")
    else:
        render_validation(True)

elif view == "📦 산출물":
    st.subheader("📦 산출물")
    if not allowed(authorizations, "READ"):
        st.error("전체 산출물 조회 권한이 없습니다.", icon=":material/lock:")
    else:
        render_artifacts(target, metadata_schema, user_id, allowed(authorizations, "EDIT"), query_frame, connect, qualified)

elif view == "🔗 매핑":
    st.subheader("🔗 매핑")
    render_mapping_workspace(maps, target, metadata_schema, user_id, lambda project, subject_area: allowed(authorizations, "EDIT", project, subject_area), query_frame, connect, qualified)

elif view == "👤 사용자":
    st.subheader("👤 사용자")
    render_user_management(target, metadata_schema, user_id, query_frame, connect, qualified)

elif maps.empty:
    st.info("테이블 매핑이 없습니다.", icon=":material/info:")

else:
    selected_id = None if view == "🧱 물리설계" else st.selectbox("테이블", maps.mpg_id.tolist(), format_func=lambda value: mapping_label(maps, value))
    selected = maps.loc[maps.mpg_id.eq(selected_id)].iloc[0] if selected_id is not None else None
    can_edit = allowed(authorizations, "EDIT", selected.prj_cd, selected.sbj_area_cd) if selected is not None else False
    can_approve = allowed(authorizations, "APRV", selected.prj_cd, selected.sbj_area_cd) if selected is not None else False
    if view == "🧱 물리설계":
        st.subheader("🧱 대상 물리설계")
        physical = physical_designs(maps)
        st.dataframe(physical, hide_index=True, height=560)
        physical_id = st.selectbox("물리설계 테이블", maps.mpg_id.tolist(), format_func=lambda value: mapping_label(maps, value), key="physical_design_table")
        physical_map = maps.loc[maps.mpg_id.eq(physical_id)].iloc[0]
        physical_edit = allowed(authorizations, "EDIT", physical_map.prj_cd, physical_map.sbj_area_cd)
        with st.form("physical_design_form"):
            target_dist_options = ["AUTO", "EVEN", "KEY", "ALL"]
            target_sort_options = ["AUTO", "NONE", "COMPOUND", "INTERLEAVED"]
            current_dist = text(physical_map.tgt_dist_style).upper() or "AUTO"
            current_sort = text(physical_map.tgt_sort_style).upper() or "AUTO"
            tgt_dist_style = st.selectbox("대상 분산 방식", target_dist_options, index=target_dist_options.index(current_dist) if current_dist in target_dist_options else 0)
            tgt_dist_key_col = st.text_input("대상 분산키", value=text(physical_map.tgt_dist_key_col))
            tgt_sort_style = st.selectbox("대상 정렬 방식", target_sort_options, index=target_sort_options.index(current_sort) if current_sort in target_sort_options else 0)
            tgt_sort_cols = st.text_input("대상 정렬키", value=text(physical_map.tgt_sort_cols))
            tgt_encd_auto_yn = st.toggle("대상 자동 압축", value=False if pd.isna(physical_map.tgt_encd_auto_yn) else bool(physical_map.tgt_encd_auto_yn))
            saved = st.form_submit_button("물리설계 저장", icon=":material/save:", type="primary", disabled=not physical_edit)
        if saved:
            try:
                if tgt_dist_style == "KEY" and not text(tgt_dist_key_col):
                    raise ValueError("대상 분산 방식 KEY에는 대상 분산키가 필요합니다.")
                if tgt_sort_style in {"COMPOUND", "INTERLEAVED"} and not text(tgt_sort_cols):
                    raise ValueError("대상 정렬 방식에는 대상 정렬키가 필요합니다.")
                save_physical_design(target, metadata_schema, int(physical_id), {"tgt_dist_style": tgt_dist_style, "tgt_dist_key_col": text(tgt_dist_key_col), "tgt_sort_style": tgt_sort_style, "tgt_sort_cols": text(tgt_sort_cols), "tgt_encd_auto_yn": tgt_encd_auto_yn}, user_id)
                st.rerun()
            except Exception as error:
                st.error(str(error), icon=":material/error:")
    elif view == "🧾 DDL":
        try:
            st.subheader("🧾 DBA 참조 DDL")
            ddl = ddl_for(selected, column_maps(target, metadata_schema, int(selected_id)))
            st.code(ddl, language="sql")
            st.download_button("DDL 다운로드", ddl, file_name=f"{text(selected.tgt_sch_nm)}_{text(selected.tgt_tbl_nm)}_reference.sql", mime="text/sql", icon=":material/download:")
        except Exception as error:
            st.error(str(error), icon=":material/error:")
    elif view == "📋 실행 이력":
        st.subheader("📋 실행 이력")
        st.dataframe(run_logs(target, metadata_schema).rename(columns={"wrk_dt": "작업일자", "dag_nm": "DAG", "dag_run_id": "DAG 실행", "task_nm": "태스크", "mpg_id": "매핑 ID", "meta_ver_no": "버전", "sql_file_path": "SQL 경로", "log_file_path": "로그 경로", "wrk_cnd_val": "작업 조건", "wrk_step_cd": "작업 단계", "wrk_sts_cd": "작업 상태", "src_row_cnt": "원천 건수", "tgt_row_cnt": "대상 건수", "src_size_byte": "원천 크기", "tgt_size_byte": "대상 크기", "wrk_stt_dtm": "시작일시", "wrk_end_dtm": "종료일시", "wrk_elps_sec": "경과초", "wrk_msg": "메시지"}), hide_index=True, height=560)
    else:
        statuses = ["DRAFT", "REVIEW", "APPROVED", "REJECTED"]
        st.subheader("🧾 승인")
        with st.form("approval_form"):
            mapping_status = st.selectbox("매핑", statuses, index=statuses.index(text(selected.mpg_sts_cd)) if text(selected.mpg_sts_cd) in statuses else 0)
            ddl_status = st.selectbox("DDL", statuses, index=statuses.index(text(selected.ddl_aprv_sts_cd)) if text(selected.ddl_aprv_sts_cd) in statuses else 0)
            saved = st.form_submit_button("승인 저장", icon=":material/task_alt:", type="primary", disabled=not can_approve)
        if saved:
            save_approval(target, metadata_schema, int(selected_id), mapping_status, ddl_status, user_id)
            st.rerun()

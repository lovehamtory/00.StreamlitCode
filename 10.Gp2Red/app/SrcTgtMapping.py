from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Callable
from uuid import uuid4

import pandas as pd
import streamlit as st

from SrcTgtArtifact import excel_bytes
from SrcTgtConnection import connection_frame, connection_label, runtime_connection_values, selectable_connections, validate_mapping_connections
from SrcTgtDataType import redshift_type
from SrcTgtDagGenerator import once_controller_source, save_dag_files
from SrcTgtLoadState import INCR_BASIS_CODES, normalize_parallel, transition_plan


TABLE_FIELDS = [
    "MPG_ID", "PRJ_CD", "SBJ_AREA_CD", "SRC_CONN_ID", "SRC_SCH_NM", "SRC_TBL_NM",
    "TGT_CONN_ID", "TGT_SCH_NM", "TGT_TBL_NM", "TGT_DIST_STYLE", "TGT_DIST_KEY_COL", "TGT_SORT_STYLE", "TGT_SORT_COLS", "TGT_ENCD_AUTO_YN", "LOAD_STS_CD", "INCR_BASIS_CD", "INCR_BASIS_COL_NM", "PARL_MTHD_CD", "PARL_CND_ARR",
]

COLUMN_FIELDS = [
    "MPG_ID", "PRJ_CD", "SBJ_AREA_CD", "SRC_CONN_ID", "SRC_SCH_NM", "SRC_TBL_NM", "TGT_CONN_ID", "TGT_SCH_NM", "TGT_TBL_NM", "COL_ORD", "SRC_COL_NO", "SRC_COL_NM", "SRC_DATA_TYPE", "SRC_NULL_YN", "SRC_KEY_ROLE_CD", "TGT_COL_NO", "TGT_COL_NM", "TGT_DATA_TYPE", "TGT_NULL_YN", "TGT_KEY_ROLE_CD", "TRNSF_EXPR", "DFLT_EXPR", "SUM_VALD_YN", "HSH_VALD_YN",
]

FIELD_LABELS = {
    "MPG_ID": "테이블매핑ID", "PRJ_CD": "프로젝트코드", "SBJ_AREA_CD": "주제영역코드", "SRC_CONN_ID": "원천접속ID", "SRC_SCH_NM": "원천스키마명", "SRC_TBL_NM": "원천테이블명",
    "TGT_CONN_ID": "대상접속ID", "TGT_SCH_NM": "대상스키마명", "TGT_TBL_NM": "대상테이블명", "TGT_DIST_STYLE": "대상분산방식", "TGT_DIST_KEY_COL": "대상분산키컬럼명", "TGT_SORT_STYLE": "대상정렬방식", "TGT_SORT_COLS": "대상정렬키컬럼목록", "TGT_ENCD_AUTO_YN": "대상자동압축여부", "LOAD_STS_CD": "적재 상태", "INCR_BASIS_CD": "증분 기준", "INCR_BASIS_COL_NM": "증분 기준 컬럼", "PARL_MTHD_CD": "S3 병렬 방식", "PARL_CND_ARR": "S3 병렬 조건",
    "COL_ORD": "매핑순서", "SRC_COL_NO": "원천컬럼순번", "SRC_COL_NM": "원천컬럼명", "SRC_DATA_TYPE": "원천데이터타입", "SRC_NULL_YN": "원천NULL허용여부", "SRC_KEY_ROLE_CD": "원천키역할코드", "TGT_COL_NO": "대상컬럼순번", "TGT_COL_NM": "대상컬럼명", "TGT_DATA_TYPE": "대상데이터타입", "TGT_NULL_YN": "대상NULL허용여부", "TGT_KEY_ROLE_CD": "대상키역할코드", "TRNSF_EXPR": "변환SQL식", "DFLT_EXPR": "기본값SQL식", "SUM_VALD_YN": "SUM검증여부", "HSH_VALD_YN": "HASH검증여부",
}

NATURAL_FIELDS = ["PRJ_CD", "SBJ_AREA_CD", "SRC_CONN_ID", "SRC_SCH_NM", "SRC_TBL_NM", "TGT_CONN_ID", "TGT_SCH_NM", "TGT_TBL_NM"]
REQUIRED_TABLE_FIELDS = ["PRJ_CD", "SBJ_AREA_CD", "SRC_CONN_ID", "SRC_SCH_NM", "SRC_TBL_NM", "TGT_CONN_ID", "TGT_SCH_NM", "TGT_TBL_NM"]
REQUIRED_COLUMN_FIELDS = ["COL_ORD", "TGT_COL_NM", "TGT_DATA_TYPE"]


def text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def boolean(value: object, default: bool = False) -> bool:
    value_text = text(value).upper()
    if not value_text:
        return default
    if value_text in {"Y", "YES", "TRUE", "1"}:
        return True
    if value_text in {"N", "NO", "FALSE", "0"}:
        return False
    raise ValueError(f"Y/N 값이 올바르지 않습니다: {value}")


def integer(value: object, field: str, required: bool = False, default: int | None = None) -> int | None:
    value_text = text(value)
    if not value_text:
        if required:
            raise ValueError(f"{FIELD_LABELS[field]}은(는) 필수입니다.")
        return default
    try:
        return int(float(value_text))
    except ValueError as error:
        raise ValueError(f"{FIELD_LABELS[field]}은(는) 정수여야 합니다.") from error


def date_value(value: object) -> object:
    value_text = text(value)
    if not value_text:
        return None
    converted = pd.to_datetime(value_text, errors="coerce")
    if pd.isna(converted):
        raise ValueError("원천기준일자는 YYYY-MM-DD 형식이어야 합니다.")
    return converted.date()


def normalize_columns(frame: pd.DataFrame, fields: list[str]) -> pd.DataFrame:
    by_name = {field.upper(): field for field in fields}
    by_name.update({label.upper(): field for field, label in FIELD_LABELS.items() if field in fields})
    renamed = {column: by_name.get(text(column).upper(), text(column).upper()) for column in frame.columns}
    result = frame.rename(columns=renamed).copy()
    duplicate = result.columns[result.columns.duplicated()].tolist()
    if duplicate:
        raise ValueError(f"중복된 업로드 컬럼이 있습니다: {', '.join(duplicate)}")
    return result


def defaults(row: dict[str, object]) -> dict[str, object]:
    output = {field: row.get(field) for field in TABLE_FIELDS}
    output["SRC_CONN_ID"] = (text(output["SRC_CONN_ID"]) or "SRC_GP").upper()
    output["TGT_CONN_ID"] = (text(output["TGT_CONN_ID"]) or "TGT_RED").upper()
    output["TGT_DIST_STYLE"] = text(output["TGT_DIST_STYLE"]).upper() or "AUTO"
    output["TGT_SORT_STYLE"] = text(output["TGT_SORT_STYLE"]).upper() or "AUTO"
    output["TGT_ENCD_AUTO_YN"] = boolean(output["TGT_ENCD_AUTO_YN"], True)
    output["LOAD_STS_CD"] = text(output["LOAD_STS_CD"]).upper().replace("INCREMENTAL", "INCR") or "FULL"
    output["INCR_BASIS_CD"] = text(output["INCR_BASIS_CD"]).upper() or None
    for field in ("TGT_DIST_KEY_COL", "TGT_SORT_COLS", "INCR_BASIS_COL_NM"):
        output[field] = text(output[field]) or None
    parallel = normalize_parallel(output["PARL_MTHD_CD"], output["PARL_CND_ARR"])
    output["PARL_MTHD_CD"] = str(parallel["method"])
    output["PARL_CND_ARR"] = json.dumps(parallel["conditions"], ensure_ascii=False) if parallel["conditions"] else None
    for field in REQUIRED_TABLE_FIELDS:
        if not text(output[field]):
            raise ValueError(f"{FIELD_LABELS[field]}은(는) 필수입니다.")
        output[field] = text(output[field])
    if output["TGT_DIST_STYLE"] not in {"AUTO", "EVEN", "KEY", "ALL"}:
        raise ValueError("대상분산방식은 AUTO, EVEN, KEY, ALL 중 하나여야 합니다.")
    if output["TGT_DIST_STYLE"] == "KEY" and not output["TGT_DIST_KEY_COL"]:
        raise ValueError("대상분산방식 KEY에는 대상분산키컬럼명이 필요합니다.")
    if output["TGT_SORT_STYLE"] not in {"AUTO", "NONE", "COMPOUND", "INTERLEAVED"}:
        raise ValueError("대상정렬방식은 AUTO, NONE, COMPOUND, INTERLEAVED 중 하나여야 합니다.")
    if output["TGT_SORT_STYLE"] in {"COMPOUND", "INTERLEAVED"} and not output["TGT_SORT_COLS"]:
        raise ValueError("대상정렬방식에는 대상정렬키컬럼목록이 필요합니다.")
    if output["LOAD_STS_CD"] not in {"FULL", "INCR"}:
        raise ValueError("적재 상태는 FULL 또는 INCR 중 하나여야 합니다.")
    if output["INCR_BASIS_CD"] and output["INCR_BASIS_CD"] not in INCR_BASIS_CODES:
        raise ValueError("증분 기준을 확인하십시오.")
    if output["LOAD_STS_CD"] == "INCR" and (output["INCR_BASIS_CD"] not in INCR_BASIS_CODES or not output["INCR_BASIS_COL_NM"]):
        raise ValueError("증분 운영 테이블에는 증분 기준과 증분 기준 컬럼이 필요합니다.")
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,7}", text(output["SBJ_AREA_CD"]).upper()):
        raise ValueError("주제영역코드는 영문으로 시작하는 영문·숫자·밑줄 1~8자리여야 합니다.")
    return output


def normalized_columns(frame: pd.DataFrame) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    target_names: set[tuple[str, str]] = set()
    target_numbers: set[tuple[str, int]] = set()
    for number, source in enumerate(frame.to_dict(orient="records"), start=2):
        row = {field: source.get(field) for field in COLUMN_FIELDS}
        for field in REQUIRED_COLUMN_FIELDS:
            if not text(row[field]):
                raise ValueError(f"컬럼매핑 {number}행의 {FIELD_LABELS[field]}은(는) 필수입니다.")
        row["COL_ORD"] = integer(row["COL_ORD"], "COL_ORD", required=True)
        row["SRC_COL_NO"] = integer(row["SRC_COL_NO"], "SRC_COL_NO")
        row["TGT_COL_NO"] = integer(row["TGT_COL_NO"], "TGT_COL_NO")
        row["SRC_NULL_YN"] = boolean(row["SRC_NULL_YN"], True)
        row["TGT_NULL_YN"] = boolean(row["TGT_NULL_YN"], True)
        row["SUM_VALD_YN"] = boolean(row["SUM_VALD_YN"], False)
        row["HSH_VALD_YN"] = boolean(row["HSH_VALD_YN"], False)
        if not integer(row["MPG_ID"], "MPG_ID") and any(not text(row[field]) for field in NATURAL_FIELDS):
            raise ValueError(f"컬럼매핑 {number}행은 테이블매핑ID 또는 원천·대상 테이블 식별값이 필요합니다.")
        for field in ("SRC_COL_NM", "SRC_DATA_TYPE", "SRC_KEY_ROLE_CD", "TGT_COL_NM", "TGT_KEY_ROLE_CD", "TRNSF_EXPR", "DFLT_EXPR"):
            row[field] = text(row[field]) or None
        row["TGT_DATA_TYPE"] = redshift_type(row["TGT_DATA_TYPE"])
        mapping_key = text(row["MPG_ID"]) or "|".join(text(row[field]) for field in NATURAL_FIELDS)
        target_name = text(row["TGT_COL_NM"]).upper()
        target_name_key = (mapping_key, target_name)
        if target_name_key in target_names:
            raise ValueError(f"동일 테이블매핑의 대상컬럼명이 중복됩니다: {target_name}")
        target_names.add(target_name_key)
        if row["TGT_COL_NO"] is not None:
            target_number_key = (mapping_key, row["TGT_COL_NO"])
            if target_number_key in target_numbers:
                raise ValueError(f"동일 테이블매핑의 대상컬럼순번이 중복됩니다: {row['TGT_COL_NO']}")
            target_numbers.add(target_number_key)
        rows.append(row)
    if not rows:
        raise ValueError("컬럼매핑을 한 건 이상 입력하십시오.")
    return rows


def natural_key(row: dict[str, object]) -> tuple[str, ...]:
    return tuple(text(row[field]) for field in NATURAL_FIELDS)


def validate_tables(frame: pd.DataFrame, can_edit: Callable[[str, str], bool]) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    keys: set[tuple[str, ...]] = set()
    for number, source in enumerate(frame.to_dict(orient="records"), start=2):
        row = defaults(source)
        row["MPG_ID"] = integer(source.get("MPG_ID"), "MPG_ID")
        key = natural_key(row)
        if key in keys:
            raise ValueError(f"테이블매핑 {number}행의 원천·대상 식별값이 중복됩니다.")
        if not can_edit(text(row["PRJ_CD"]), text(row["SBJ_AREA_CD"])):
            raise ValueError(f"테이블매핑 {number}행의 수정 권한이 없습니다.")
        keys.add(key)
        result.append(row)
    if not result:
        raise ValueError("테이블매핑을 한 건 이상 입력하십시오.")
    return result


def mapping_id(cursor: Any, schema_name: str, qualified: Callable[[str, str], str], row: dict[str, object]) -> int | None:
    table_name = qualified(schema_name, "tb_mig_tbl_mpg")
    if row.get("MPG_ID"):
        cursor.execute(f"SELECT mpg_id FROM {table_name} WHERE mpg_id = %s AND active_yn = TRUE", (row["MPG_ID"],))
        found = cursor.fetchall()
    else:
        cursor.execute(f"SELECT mpg_id FROM {table_name} WHERE prj_cd = %s AND sbj_area_cd = %s AND src_conn_id = %s AND src_sch_nm = %s AND src_tbl_nm = %s AND tgt_conn_id = %s AND tgt_sch_nm = %s AND tgt_tbl_nm = %s AND active_yn = TRUE", natural_key(row))
        found = cursor.fetchall()
    if len(found) > 1:
        raise ValueError("동일 원천·대상 식별값의 활성 테이블매핑이 둘 이상입니다.")
    return int(found[0][0]) if found else None


def upsert_table(cursor: Any, schema_name: str, qualified: Callable[[str, str], str], row: dict[str, object]) -> int:
    table_name = qualified(schema_name, "tb_mig_tbl_mpg")
    current_id = mapping_id(cursor, schema_name, qualified, row)
    insert_fields = TABLE_FIELDS[1:]
    insert_row = dict(row)
    insert_row["LOAD_STS_CD"] = "FULL"
    columns = [field.lower() for field in insert_fields]
    values = tuple(insert_row[field] for field in insert_fields)
    if current_id is None:
        cursor.execute(
            f"INSERT INTO {table_name} ({', '.join(columns)}, active_yn) VALUES ({', '.join('%s' for _ in values)}, TRUE)",
            values,
        )
        current_id = mapping_id(cursor, schema_name, qualified, row)
        if current_id is None:
            raise RuntimeError("신규 테이블매핑 ID를 확인할 수 없습니다.")
        return current_id
    update_fields = [field for field in insert_fields if field != "LOAD_STS_CD"]
    update_columns = [field.lower() for field in update_fields]
    cursor.execute(f"UPDATE {table_name} SET {', '.join(f'{column} = %s' for column in update_columns)}, tgt_ddl_sql = NULL, meta_ver_no = meta_ver_no + 1, upd_dtm = GETDATE() WHERE mpg_id = %s", (*tuple(row[field] for field in update_fields), current_id))
    return current_id


def resolve_column_mapping_id(cursor: Any, schema_name: str, qualified: Callable[[str, str], str], row: dict[str, object], uploaded_ids: dict[tuple[str, ...], int]) -> int:
    supplied = integer(row.get("MPG_ID"), "MPG_ID")
    if supplied:
        return supplied
    key = natural_key(row)
    if key in uploaded_ids:
        return uploaded_ids[key]
    lookup = {field: row.get(field) for field in TABLE_FIELDS}
    lookup.update({field: row.get(field) for field in NATURAL_FIELDS})
    resolved = mapping_id(cursor, schema_name, qualified, lookup)
    if resolved is None:
        raise ValueError(f"컬럼매핑의 대상 테이블매핑을 찾을 수 없습니다: {key[0]} / {key[1]} / {key[4]} → {key[7]}")
    return resolved


def mapping_scope(cursor: Any, schema_name: str, qualified: Callable[[str, str], str], mapping_id_value: int) -> tuple[str, str]:
    cursor.execute(f"SELECT prj_cd, sbj_area_cd FROM {qualified(schema_name, 'tb_mig_tbl_mpg')} WHERE mpg_id = %s AND active_yn = TRUE", (mapping_id_value,))
    found = cursor.fetchall()
    if len(found) != 1:
        raise ValueError(f"컬럼매핑의 활성 테이블매핑을 찾을 수 없습니다: {mapping_id_value}")
    return text(found[0][0]), text(found[0][1])


def save_bundle(connect: Callable[[dict[str, Any]], Any], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], tables: list[dict[str, object]], columns: list[dict[str, object]], replace_columns: bool, can_edit: Callable[[str, str], bool]) -> tuple[int, int]:
    table_name = qualified(schema_name, "tb_mig_col_mpg")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            subject_table = qualified(schema_name, "tb_mig_sbj_area")
            for subject_area in sorted({text(row["SBJ_AREA_CD"]) for row in tables}):
                cursor.execute(f"SELECT 1 FROM {subject_table} WHERE sbj_area_cd = %s AND up_sbj_area_cd IS NOT NULL AND active_yn = TRUE", (subject_area,))
                if cursor.fetchone() is None:
                    raise ValueError(f"사용 중인 실행 주제영역을 찾을 수 없습니다: {subject_area}")
            for row in tables:
                validate_mapping_connections(cursor, schema_name, qualified, row["SRC_CONN_ID"], row["TGT_CONN_ID"])
            uploaded_ids = {natural_key(row): upsert_table(cursor, schema_name, qualified, row) for row in tables}
            uploaded_scopes = {uploaded_ids[natural_key(row)]: (text(row["PRJ_CD"]), text(row["SBJ_AREA_CD"])) for row in tables}
            mapped_columns: list[tuple[int, dict[str, object]]] = []
            seen_orders: set[tuple[int, int]] = set()
            for row in columns:
                map_id = resolve_column_mapping_id(cursor, schema_name, qualified, row, uploaded_ids)
                scope = uploaded_scopes.get(map_id)
                if scope is None:
                    scope = mapping_scope(cursor, schema_name, qualified, map_id)
                project, subject_area = scope
                if not can_edit(project, subject_area):
                    raise ValueError(f"컬럼매핑의 수정 권한이 없습니다: {project} / {subject_area}")
                key = (map_id, int(row["COL_ORD"]))
                if key in seen_orders:
                    raise ValueError(f"동일 테이블매핑의 매핑순서가 중복됩니다: {map_id} / {row['COL_ORD']}")
                seen_orders.add(key)
                mapped_columns.append((map_id, row))
            if replace_columns:
                for map_id in sorted({map_id for map_id, _ in mapped_columns}):
                    cursor.execute(f"UPDATE {table_name} SET active_yn = FALSE, upd_dtm = GETDATE() WHERE mpg_id = %s AND active_yn = TRUE", (map_id,))
            for map_id, row in mapped_columns:
                if not replace_columns:
                    cursor.execute(f"UPDATE {table_name} SET active_yn = FALSE, upd_dtm = GETDATE() WHERE mpg_id = %s AND col_ord = %s AND active_yn = TRUE", (map_id, row["COL_ORD"]))
                cursor.execute(
                    f'''INSERT INTO {table_name} (mpg_id, col_ord, src_col_no, src_col_nm, src_data_type, src_null_yn, src_key_role_cd, tgt_col_no, tgt_col_nm, tgt_data_type, tgt_null_yn, tgt_key_role_cd, trnsf_expr, dflt_expr, sum_vald_yn, hsh_vald_yn, active_yn) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)''',
                    (map_id, row["COL_ORD"], row["SRC_COL_NO"], row["SRC_COL_NM"], row["SRC_DATA_TYPE"], row["SRC_NULL_YN"], row["SRC_KEY_ROLE_CD"], row["TGT_COL_NO"], row["TGT_COL_NM"], row["TGT_DATA_TYPE"], row["TGT_NULL_YN"], row["TGT_KEY_ROLE_CD"], row["TRNSF_EXPR"], row["DFLT_EXPR"], row["SUM_VALD_YN"], row["HSH_VALD_YN"]),
                )
        connection.commit()
    return len(tables), len(columns)


def template_frame(fields: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=[FIELD_LABELS[field] for field in fields])


def load_upload(uploaded: Any) -> tuple[pd.DataFrame, pd.DataFrame]:
    try:
        tables = pd.read_excel(uploaded, sheet_name="테이블매핑", dtype=object)
        columns = pd.read_excel(uploaded, sheet_name="컬럼매핑", dtype=object)
    except Exception as error:
        raise ValueError("업로드 파일에는 테이블매핑, 컬럼매핑 시트가 모두 필요합니다.") from error
    return normalize_columns(tables, TABLE_FIELDS), normalize_columns(columns, COLUMN_FIELDS)


def existing_columns(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], mapping_id: int) -> pd.DataFrame:
    query = f'''SELECT col_ord AS "COL_ORD", src_col_no AS "SRC_COL_NO", src_col_nm AS "SRC_COL_NM", src_data_type AS "SRC_DATA_TYPE", src_null_yn AS "SRC_NULL_YN", src_key_role_cd AS "SRC_KEY_ROLE_CD", tgt_col_no AS "TGT_COL_NO", tgt_col_nm AS "TGT_COL_NM", tgt_data_type AS "TGT_DATA_TYPE", tgt_null_yn AS "TGT_NULL_YN", tgt_key_role_cd AS "TGT_KEY_ROLE_CD", trnsf_expr AS "TRNSF_EXPR", dflt_expr AS "DFLT_EXPR", sum_vald_yn AS "SUM_VALD_YN", hsh_vald_yn AS "HSH_VALD_YN"
                  FROM {qualified(schema_name, "tb_mig_col_mpg")}
                 WHERE mpg_id = %s AND active_yn = TRUE
                 ORDER BY col_ord'''
    return query_frame(values, query, (mapping_id,))


def form_value(row: pd.Series | None, field: str, default: str = "") -> str:
    return text(row[field.lower()]) if row is not None and field.lower() in row.index else default


def source_layout_table(values: dict[str, Any]) -> tuple[str, str]:
    settings = dict(st.secrets.get("layout_history", {}))
    return text(settings.get("schema")) or text(values.get("default_schema")) or "public", text(settings.get("table")) or "TB_TABLE_LAYOUT_GP"


def source_snapshots(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], qualified: Callable[[str, str], str]) -> pd.DataFrame:
    schema_name, table_name = source_layout_table(values)
    query = f'''SELECT COALESCE(src_conn_id, 'SRC_GP') AS "SRC_CONN_ID", std_dt AS "STD_DT", owner AS "OWNER", tbl AS "TBL", MAX(entity) AS "ENTITY"
                  FROM {qualified(schema_name, table_name)}
                 GROUP BY COALESCE(src_conn_id, 'SRC_GP'), std_dt, owner, tbl
                 ORDER BY std_dt DESC, src_conn_id, owner, tbl'''
    return query_frame(values, query)


def source_columns(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], qualified: Callable[[str, str], str], source_connection_id: str, standard_date: str, owner: str, table: str, character_multiple: object = 3) -> pd.DataFrame:
    schema_name, table_name = source_layout_table(values)
    query = f'''SELECT colno AS "COL_ORD", colno AS "SRC_COL_NO", col AS "SRC_COL_NM", datatype AS "SRC_DATA_TYPE", len AS "SRC_DATA_LEN",
                      CASE WHEN UPPER(COALESCE(nullable, 'YES')) IN ('NO', 'N', 'FALSE') THEN FALSE ELSE TRUE END AS "SRC_NULL_YN",
                      CASE WHEN UPPER(COALESCE(ispk, '')) IN ('Y', 'YES', 'TRUE') THEN 'PK' ELSE NULL END AS "SRC_KEY_ROLE_CD"
                  FROM {qualified(schema_name, table_name)}
                 WHERE COALESCE(src_conn_id, 'SRC_GP') = %s AND std_dt = %s AND owner = %s AND tbl = %s
                 ORDER BY colno'''
    source = query_frame(values, query, (source_connection_id, standard_date, owner, table))
    if source.empty:
        raise ValueError("선택한 원천 레이아웃의 컬럼을 찾을 수 없습니다.")
    result = pd.DataFrame(columns=COLUMN_FIELDS[9:])
    for column in source.columns:
        result[column] = source[column]
    result["TGT_COL_NO"] = source["COL_ORD"]
    result["TGT_COL_NM"] = source["SRC_COL_NM"]
    source_lengths = source["SRC_DATA_LEN"] if "SRC_DATA_LEN" in source.columns else pd.Series([None] * len(source), index=source.index)
    result["TGT_DATA_TYPE"] = [redshift_type(data_type, data_length, character_multiple) for data_type, data_length in zip(source["SRC_DATA_TYPE"], source_lengths)]
    result["TGT_NULL_YN"] = source["SRC_NULL_YN"]
    result["TGT_KEY_ROLE_CD"] = source["SRC_KEY_ROLE_CD"]
    result["TRNSF_EXPR"] = None
    result["DFLT_EXPR"] = None
    result["SUM_VALD_YN"] = False
    result["HSH_VALD_YN"] = False
    return result.loc[:, COLUMN_FIELDS[9:]]


def target_columns(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, table_name: str) -> pd.DataFrame:
    query = '''SELECT c.ordinal_position AS "TGT_COL_NO", c.column_name AS "TGT_COL_NM", c.data_type AS "TGT_DATA_TYPE",
                      CASE WHEN c.is_nullable = 'YES' THEN TRUE ELSE FALSE END AS "TGT_NULL_YN",
                      CASE WHEN pk.column_name IS NULL THEN NULL ELSE 'PK' END AS "TGT_KEY_ROLE_CD"
                 FROM information_schema.columns c
                 LEFT JOIN (
                     SELECT kcu.table_schema, kcu.table_name, kcu.column_name
                       FROM information_schema.table_constraints tc
                       JOIN information_schema.key_column_usage kcu
                         ON kcu.constraint_name = tc.constraint_name
                        AND kcu.constraint_schema = tc.constraint_schema
                        AND kcu.table_name = tc.table_name
                      WHERE tc.constraint_type = 'PRIMARY KEY'
                 ) pk ON pk.table_schema = c.table_schema AND pk.table_name = c.table_name AND pk.column_name = c.column_name
                WHERE c.table_schema = %s AND c.table_name = %s
                ORDER BY c.ordinal_position'''
    result = query_frame(values, query, (schema_name, table_name))
    if result.empty:
        raise ValueError("대상 테이블을 찾을 수 없거나 조회 권한이 없습니다.")
    return result


def automatic_columns(source: pd.DataFrame, target: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    target_by_name = {text(row.TGT_COL_NM).upper(): row for row in target.itertuples(index=False)}
    result = source.copy()
    matched = 0
    for index, row in result.iterrows():
        target_row = target_by_name.get(text(row["SRC_COL_NM"]).upper())
        if target_row is None:
            result.loc[index, ["TGT_COL_NO", "TGT_COL_NM", "TGT_DATA_TYPE", "TGT_NULL_YN", "TGT_KEY_ROLE_CD"]] = [None, None, None, True, None]
            continue
        result.at[index, "TGT_COL_NO"] = target_row.TGT_COL_NO
        result.at[index, "TGT_COL_NM"] = target_row.TGT_COL_NM
        result.at[index, "TGT_DATA_TYPE"] = target_row.TGT_DATA_TYPE
        result.at[index, "TGT_NULL_YN"] = target_row.TGT_NULL_YN
        result.at[index, "TGT_KEY_ROLE_CD"] = target_row.TGT_KEY_ROLE_CD
        matched += 1
    return result, matched


def render_single(maps: pd.DataFrame, values: dict[str, Any], schema_name: str, can_edit: Callable[[str, str], bool], query_frame: Callable[..., pd.DataFrame], connect: Callable[[dict[str, Any]], Any], qualified: Callable[[str, str], str]) -> None:
    options = ["신규", *[int(value) for value in maps.mpg_id.tolist()]]
    selected = st.selectbox("테이블매핑", options, format_func=lambda value: "신규 테이블매핑" if value == "신규" else f"{value} · {maps.loc[maps.mpg_id.eq(value)].iloc[0].src_sch_nm}.{maps.loc[maps.mpg_id.eq(value)].iloc[0].src_tbl_nm} → {maps.loc[maps.mpg_id.eq(value)].iloc[0].tgt_sch_nm}.{maps.loc[maps.mpg_id.eq(value)].iloc[0].tgt_tbl_nm}")
    current = None if selected == "신규" else maps.loc[maps.mpg_id.eq(selected)].iloc[0]
    try:
        connections = connection_frame(query_frame, values, schema_name, qualified, active_only=True)
        source_connections = selectable_connections(connections, None if current is None else current.src_conn_id)
        target_connections = selectable_connections(connections, None if current is None else current.tgt_conn_id)
        if source_connections.empty or target_connections.empty:
            raise ValueError("사용 중인 원천·대상 접속정보를 각각 한 건 이상 등록하십시오.")
    except Exception as error:
        st.error(f"접속정보 조회 실패: {error}", icon=":material/error:")
        return
    source_snapshot = None
    automatic_key = f"mapping_auto_columns_{selected}"
    if current is None:
        try:
            snapshots = source_snapshots(query_frame, values, qualified)
            snapshots = snapshots.loc[snapshots.SRC_CONN_ID.map(text).str.upper().isin(source_connections.conn_id.map(text).str.upper())].copy()
            if snapshots.empty:
                raise ValueError("사용 중인 원천 접속정보의 레이아웃 적재 이력이 없습니다. 먼저 원천 레이아웃에서 기준일을 적재하십시오.")
            snapshot_options = snapshots[["SRC_CONN_ID", "STD_DT", "OWNER", "TBL"]].astype(str).agg(" | ".join, axis=1).tolist()
            selected_snapshot = st.selectbox("원천 레이아웃", snapshot_options, key="new_mapping_source")
            source_snapshot = snapshots.iloc[snapshot_options.index(selected_snapshot)]
            source_connection = source_connections.loc[source_connections.conn_id.map(text).str.upper().eq(text(source_snapshot.SRC_CONN_ID).upper())].iloc[0]
            character_multiple = int(source_connection.get("char_len_mul", 3) or 3)
            existing = source_columns(query_frame, values, qualified, text(source_snapshot.SRC_CONN_ID), text(source_snapshot.STD_DT), text(source_snapshot.OWNER), text(source_snapshot.TBL), character_multiple)
            st.caption(f"원천 구조 · {text(source_snapshot.ENTITY) or '-'} · {len(existing):,} 컬럼 · 문자길이배수 {character_multiple}배")
        except Exception as error:
            st.error(f"원천 레이아웃 조회 실패: {error}", icon=":material/error:")
            return
    else:
        existing = existing_columns(query_frame, values, schema_name, qualified, int(selected))
    if automatic_key in st.session_state:
        existing = st.session_state[automatic_key].copy()
    with st.form("single_mapping_form"):
        basic, execution = st.columns(2)
        with basic:
            prj_cd = st.text_input("프로젝트코드", value=form_value(current, "PRJ_CD"))
            sbj_area_cd = st.text_input("주제영역코드", value=form_value(current, "SBJ_AREA_CD"))
            if source_snapshot is not None:
                src_conn_id = text(source_snapshot.SRC_CONN_ID).upper()
                st.text_input("원천접속ID", value=connection_label(source_connections, src_conn_id), disabled=True)
            else:
                source_default = form_value(current, "SRC_CONN_ID", text(source_connections.iloc[0].conn_id)).upper()
                src_conn_id = st.selectbox("원천접속ID", source_connections.conn_id.tolist(), index=source_connections.conn_id.tolist().index(source_default) if source_default in source_connections.conn_id.tolist() else 0, format_func=lambda value: connection_label(source_connections, value))
            src_sch_nm = st.text_input("원천스키마명", value=text(source_snapshot.OWNER) if source_snapshot is not None else form_value(current, "SRC_SCH_NM"), disabled=True)
            src_tbl_nm = st.text_input("원천테이블명", value=text(source_snapshot.TBL) if source_snapshot is not None else form_value(current, "SRC_TBL_NM"), disabled=True)
            target_default = form_value(current, "TGT_CONN_ID", text(target_connections.iloc[0].conn_id)).upper()
            tgt_conn_id = st.selectbox("대상접속ID", target_connections.conn_id.tolist(), index=target_connections.conn_id.tolist().index(target_default) if target_default in target_connections.conn_id.tolist() else 0, format_func=lambda value: connection_label(target_connections, value))
            tgt_sch_nm = st.text_input("대상스키마명", value=form_value(current, "TGT_SCH_NM"))
            tgt_tbl_nm = st.text_input("대상테이블명", value=form_value(current, "TGT_TBL_NM"))
        with execution:
            st.selectbox("적재 상태", [form_value(current, "LOAD_STS_CD", "FULL") or "FULL"], disabled=True)
            st.caption("신규 테이블은 전체 적재로 등록됩니다. 증분 전환은 별도 전환 화면에서만 수행합니다.")
            incr_basis_cd = st.selectbox("증분 기준", ["", "DT", "YMD", "YM", "WM_DTM", "PK"], index=["", "DT", "YMD", "YM", "WM_DTM", "PK"].index(form_value(current, "INCR_BASIS_CD")) if form_value(current, "INCR_BASIS_CD") in {"", "DT", "YMD", "YM", "WM_DTM", "PK"} else 0)
            incr_basis_col_nm = st.text_input("증분 기준 컬럼", value=form_value(current, "INCR_BASIS_COL_NM"))
            parl_mthd_cd = st.selectbox("S3 병렬 방식", ["NONE", "WHERE"], index=["NONE", "WHERE"].index(form_value(current, "PARL_MTHD_CD", "NONE").upper()) if form_value(current, "PARL_MTHD_CD", "NONE").upper() in {"NONE", "WHERE"} else 0)
            parl_cnd_arr = st.text_area("S3 병렬 조건", value=form_value(current, "PARL_CND_ARR"), disabled=parl_mthd_cd != "WHERE", placeholder='["abc_dt BETWEEN \'19000101\' AND \'20001231\'", "abc_dt BETWEEN \'20010101\' AND \'20101231\'"]')
            st.caption("WHERE 방식은 배열의 각 조건을 원천 조회 WHERE절에 AND로 추가해 S3 추출만 병렬 실행합니다. 배열 개수가 병렬 작업 수이며 INS는 테이블별 단일 실행입니다.")
            st.caption("S3는 항상 Parquet으로 생성하며, S3 기준경로는 대상접속정보에서 읽고 하위 경로는 테이블매핑으로 자동 계산합니다.")
        st.markdown("##### 컬럼 매핑")
        edited = st.data_editor(existing, num_rows="dynamic", hide_index=True, key=f"single_column_editor_{selected}", column_config={"COL_ORD": st.column_config.NumberColumn("매핑순서", min_value=1, step=1), "SRC_COL_NO": st.column_config.NumberColumn("원천컬럼순번", min_value=1, step=1), "SRC_COL_NM": "원천컬럼명", "SRC_DATA_TYPE": "원천데이터타입", "SRC_NULL_YN": st.column_config.CheckboxColumn("원천NULL허용여부"), "SRC_KEY_ROLE_CD": "원천키역할코드", "TGT_COL_NO": st.column_config.NumberColumn("대상컬럼순번", min_value=1, step=1), "TGT_COL_NM": "대상컬럼명", "TGT_DATA_TYPE": "대상데이터타입", "TGT_NULL_YN": st.column_config.CheckboxColumn("대상NULL허용여부"), "TGT_KEY_ROLE_CD": "대상키역할코드", "TRNSF_EXPR": "변환SQL식", "DFLT_EXPR": "기본값SQL식", "SUM_VALD_YN": st.column_config.CheckboxColumn("SUM검증여부"), "HSH_VALD_YN": st.column_config.CheckboxColumn("HASH검증여부")})
        automatic = st.form_submit_button("대상 구조 자동 반영", icon=":material/auto_fix_high:")
        submitted = st.form_submit_button("테이블·컬럼 매핑 저장", icon=":material/save:", type="primary")
    if automatic:
        try:
            target_structure = target_columns(query_frame, runtime_connection_values(target_connections, tgt_conn_id), text(tgt_sch_nm), text(tgt_tbl_nm))
            converted, matched = automatic_columns(existing, target_structure)
            st.session_state[automatic_key] = converted
            st.toast(f"대상 컬럼 {matched:,}건을 자동 반영했습니다.", icon=":material/auto_fix_high:")
            st.rerun()
        except Exception as error:
            st.error(f"대상 구조 자동 반영 실패: {error}", icon=":material/error:")
    if submitted:
        try:
            source = {} if current is None else {field: current[field.lower()] for field in TABLE_FIELDS if field.lower() in current.index}
            source.update({"MPG_ID": None if current is None else int(selected), "PRJ_CD": prj_cd, "SBJ_AREA_CD": sbj_area_cd, "SRC_CONN_ID": src_conn_id, "SRC_SCH_NM": src_sch_nm, "SRC_TBL_NM": src_tbl_nm, "TGT_CONN_ID": tgt_conn_id, "TGT_SCH_NM": tgt_sch_nm, "TGT_TBL_NM": tgt_tbl_nm, "LOAD_STS_CD": "FULL" if current is None else form_value(current, "LOAD_STS_CD", "FULL"), "INCR_BASIS_CD": incr_basis_cd, "INCR_BASIS_COL_NM": incr_basis_col_nm, "PARL_MTHD_CD": parl_mthd_cd, "PARL_CND_ARR": parl_cnd_arr})
            table = defaults(source)
            table["MPG_ID"] = None if current is None else int(selected)
            if not can_edit(text(table["PRJ_CD"]), text(table["SBJ_AREA_CD"])):
                raise ValueError("해당 프로젝트·주제영역의 수정 권한이 없습니다.")
            column_input = edited.copy()
            for field in NATURAL_FIELDS:
                column_input[field] = table[field]
            column_input["MPG_ID"] = table["MPG_ID"]
            columns = normalized_columns(column_input)
            save_bundle(connect, values, schema_name, qualified, [table], columns, True, can_edit)
            st.session_state.pop(automatic_key, None)
            st.rerun()
        except Exception as error:
            st.error(f"매핑 저장 실패: {error}", icon=":material/error:")


def render_upload(values: dict[str, Any], schema_name: str, can_edit: Callable[[str, str], bool], connect: Callable[[dict[str, Any]], Any], qualified: Callable[[str, str], str]) -> None:
    st.download_button("업로드 양식 다운로드", data=excel_bytes([("테이블매핑", template_frame(TABLE_FIELDS)), ("컬럼매핑", template_frame(COLUMN_FIELDS))]), file_name="이관매핑_업로드양식.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", icon=":material/download:")
    uploaded = st.file_uploader("이관 매핑 파일", type=["xlsx"], key="mapping_upload_file")
    replace_columns = st.toggle("업로드 테이블의 기존 컬럼매핑 교체", value=True)
    if uploaded is None:
        return
    try:
        table_frame, column_frame = load_upload(uploaded)
        st.dataframe(table_frame.rename(columns=FIELD_LABELS), hide_index=True, height=220)
        st.dataframe(column_frame.rename(columns=FIELD_LABELS), hide_index=True, height=260)
    except Exception as error:
        st.error(str(error), icon=":material/error:")
        return
    if st.button("일괄 반영", icon=":material/upload:", type="primary"):
        try:
            tables = validate_tables(table_frame, can_edit)
            columns = normalized_columns(column_frame)
            saved_tables, saved_columns = save_bundle(connect, values, schema_name, qualified, tables, columns, replace_columns, can_edit)
            st.success(f"테이블매핑 {saved_tables:,}건, 컬럼매핑 {saved_columns:,}건을 반영했습니다.", icon=":material/check_circle:")
        except Exception as error:
            st.error(f"일괄 반영 실패: {error}", icon=":material/error:")


def render_load_transition(maps: pd.DataFrame, values: dict[str, Any], schema_name: str, can_edit: Callable[[str, str], bool], connect: Callable[[dict[str, Any]], Any], qualified: Callable[[str, str], str]) -> None:
    if maps.empty:
        st.info("전환할 테이블매핑이 없습니다.", icon=":material/info:")
        return
    labels = {int(row.mpg_id): f"{int(row.mpg_id)} · {text(row.src_sch_nm)}.{text(row.src_tbl_nm)} → {text(row.tgt_sch_nm)}.{text(row.tgt_tbl_nm)} · {text(row.load_sts_cd)}" for row in maps.itertuples(index=False)}
    selected = st.multiselect("전환 테이블", list(labels), format_func=lambda value: labels[value])
    target = st.selectbox("전환 적재방식", ["FULL", "INCR"], format_func=lambda value: "전체 적재" if value == "FULL" else "증분 적재")
    reason = st.text_area("전환사유", max_chars=1000)
    if not selected:
        return
    selected_rows = maps.loc[maps.mpg_id.isin(selected)].copy()
    same = selected_rows.loc[selected_rows.load_sts_cd.map(text).str.upper().eq(target)]
    if not same.empty:
        st.warning(f"이미 {target}인 테이블은 전환할 수 없습니다: " + ", ".join(str(int(value)) for value in same.mpg_id.tolist()), icon=":material/warning:")
    if st.button("상태 전환 반영", type="primary", icon=":material/sync:"):
        try:
            if not reason.strip():
                raise ValueError("전환사유를 입력하십시오.")
            table_name = qualified(schema_name, "tb_mig_tbl_mpg")
            manifest_name = qualified(schema_name, "tb_mig_s3_manf")
            history_name = qualified(schema_name, "tb_mig_tbl_load_hist")
            log_name = qualified(schema_name, "tb_mig_run_log")
            changed = 0
            with connect(values) as connection:
                with connection.cursor() as cursor:
                    for mapping_id_value in selected:
                        row = selected_rows.loc[selected_rows.mpg_id.eq(mapping_id_value)].iloc[0]
                        if not can_edit(text(row.prj_cd), text(row.sbj_area_cd)):
                            raise ValueError(f"테이블매핑 {mapping_id_value}의 수정 권한이 없습니다.")
                        cursor.execute(f"SELECT load_sts_cd, incr_basis_cd, incr_basis_col_nm FROM {table_name} WHERE mpg_id = %s AND active_yn = TRUE", (mapping_id_value,))
                        current = cursor.fetchone()
                        if current is None:
                            raise ValueError(f"활성 테이블매핑을 찾을 수 없습니다: {mapping_id_value}")
                        cursor.execute(f"SELECT manf_id FROM {manifest_name} WHERE mpg_id = %s AND src_s3_vald_sts_cd = 'SUCCESS' ORDER BY manf_id DESC LIMIT 1", (mapping_id_value,))
                        baseline = cursor.fetchone()
                        cursor.execute(f"SELECT 1 FROM {log_name} WHERE mpg_id = %s AND wrk_sts_cd = 'RUNNING' LIMIT 1", (mapping_id_value,))
                        running = cursor.fetchone() is not None
                        plan = transition_plan(current[0], target, None if baseline is None else baseline[0], running, current[1], current[2])
                        cursor.execute(f"UPDATE {table_name} SET load_sts_cd = %s, upd_dtm = GETDATE() WHERE mpg_id = %s", (plan["after"], mapping_id_value))
                        cursor.execute(f"INSERT INTO {history_name} (mpg_id, bf_load_sts_cd, af_load_sts_cd, base_manf_id, trns_rsn) VALUES (%s, %s, %s, %s, %s)", (mapping_id_value, plan["before"], plan["after"], plan["baseline_manifest_id"], reason.strip()))
                        changed += 1
                connection.commit()
            st.success(f"테이블매핑 {changed:,}건의 적재상태를 전환했습니다.", icon=":material/check_circle:")
            st.rerun()
        except Exception as error:
            st.error(f"상태 전환 실패: {error}", icon=":material/error:")


def render_one_time_execution(maps: pd.DataFrame, values: dict[str, Any], schema_name: str, can_edit: Callable[[str, str], bool], query_frame: Callable[..., pd.DataFrame], connect: Callable[[dict[str, Any]], Any], qualified: Callable[[str, str], str]) -> None:
    candidates = maps.copy()
    if candidates.empty:
        st.info("일회성으로 실행할 테이블매핑이 없습니다.", icon=":material/info:")
        return
    labels = {int(row.mpg_id): f"{int(row.mpg_id)} · {text(row.src_sch_nm)}.{text(row.src_tbl_nm)} → {text(row.tgt_sch_nm)}.{text(row.tgt_tbl_nm)}" for row in candidates.itertuples(index=False)}
    selected = st.multiselect("일회성 실행 테이블", list(labels), format_func=lambda value: labels[value])
    run_mode = st.selectbox("실행 범위", ["S3_ONLY", "S3_INS"], format_func=lambda value: "S3 추출·검증까지" if value == "S3_ONLY" else "S3 추출·검증·대상 적재·검증")
    reason = st.text_area("일회성 실행 사유", max_chars=1000)
    if selected:
        details = candidates.loc[candidates.mpg_id.isin(selected), ["mpg_id", "src_sch_nm", "src_tbl_nm", "tgt_sch_nm", "tgt_tbl_nm"]].copy()
        details["PARL_MTHD_CD"] = "전체"
        details["PARL_CND_ARR"] = None
        details = details.rename(columns={"mpg_id": "매핑ID", "src_sch_nm": "원천스키마", "src_tbl_nm": "원천테이블", "tgt_sch_nm": "대상스키마", "tgt_tbl_nm": "대상테이블", "PARL_MTHD_CD": "S3추출방식", "PARL_CND_ARR": "S3추출병렬조건배열"})
        edited_details = st.data_editor(details, hide_index=True, disabled=["매핑ID", "원천스키마", "원천테이블", "대상스키마", "대상테이블"], column_config={"S3추출방식": st.column_config.SelectboxColumn("S3추출방식", options=["전체", "WHERE 병렬"], required=True), "S3추출병렬조건배열": st.column_config.TextColumn("S3추출병렬조건배열", help="WHERE 병렬일 때만 JSON 배열로 입력합니다.")}, key="once_work_table_editor")
        st.caption("NONE은 테이블 전체를 S3로 추출합니다. WHERE는 배열의 각 조건을 병렬 S3 작업으로 실행합니다. INS는 항상 테이블별 단일 실행입니다.")
    else:
        edited_details = pd.DataFrame()
    if st.button("일회성 DAG 생성", type="primary", icon=":material/terminal:"):
        try:
            if not selected:
                raise ValueError("일회성 실행 테이블을 선택하십시오.")
            if not reason.strip():
                raise ValueError("일회성 실행 사유를 입력하십시오.")
            if run_mode not in {"S3_ONLY", "S3_INS"}:
                raise ValueError("실행 범위를 선택하십시오.")
            log_name = qualified(schema_name, "tb_mig_run_log")
            details_by_mapping = {int(row["매핑ID"]): row for row in edited_details.to_dict(orient="records")}
            once_work_id = f"ONCE_{datetime.now():%Y%m%d%H%M%S}_{uuid4().hex[:6].upper()}"
            table_configs: list[dict[str, Any]] = []
            with connect(values) as connection:
                with connection.cursor() as cursor:
                    for mapping_id_value in selected:
                        row = candidates.loc[candidates.mpg_id.eq(mapping_id_value)].iloc[0]
                        if not can_edit(text(row.prj_cd), text(row.sbj_area_cd)):
                            raise ValueError(f"테이블매핑 {mapping_id_value}의 수정 권한이 없습니다.")
                        cursor.execute(f"SELECT 1 FROM {log_name} WHERE mpg_id = %s AND wrk_sts_cd = 'RUNNING' LIMIT 1", (mapping_id_value,))
                        running = cursor.fetchone() is not None
                        if running:
                            raise ValueError(f"실행 중인 테이블은 일회성 작업에 추가할 수 없습니다: {mapping_id_value}")
                        detail = details_by_mapping[int(mapping_id_value)]
                        parallel = normalize_parallel("NONE" if detail["S3추출방식"] == "전체" else "WHERE", detail["S3추출병렬조건배열"])
                        table_configs.append({"mpg_id": int(mapping_id_value), "parl_mthd_cd": parallel["method"], "parl_cnd_arr": parallel["conditions"] or None})
            dag_source = once_controller_source(once_work_id, run_mode, table_configs, reason.strip())
            compile(dag_source, f"{once_work_id}.py", "exec")
            paths = save_dag_files(once_work_id, {f"mig_{once_work_id.lower()}_ctl": dag_source})
            st.success(f"일회성 DAG를 생성했습니다: {paths[0].name}. 작업 조건·사유는 기존 실행로그에 기록됩니다. Airflow DAG 폴더에 배포 후 해당 DAG만 실행하십시오.", icon=":material/check_circle:")
            st.rerun()
        except Exception as error:
            st.error(f"일회성 DAG 생성 실패: {error}", icon=":material/error:")


def render_mapping_workspace(maps: pd.DataFrame, values: dict[str, Any], schema_name: str, can_edit: Callable[[str, str], bool], query_frame: Callable[..., pd.DataFrame], connect: Callable[[dict[str, Any]], Any], qualified: Callable[[str, str], str]) -> None:
    mode = st.segmented_control("매핑 업무", ["📝 단건 등록·수정", "🔄 적재상태 전환", "📤 일괄 업로드"], default="📝 단건 등록·수정", label_visibility="collapsed")
    if mode == "📝 단건 등록·수정":
        render_single(maps, values, schema_name, can_edit, query_frame, connect, qualified)
    elif mode == "🔄 적재상태 전환":
        render_load_transition(maps, values, schema_name, can_edit, connect, qualified)
    else:
        render_upload(values, schema_name, can_edit, connect, qualified)

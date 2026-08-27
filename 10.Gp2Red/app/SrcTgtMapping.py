from __future__ import annotations

import json
import re
from typing import Any, Callable

import pandas as pd
import sqlglot
import streamlit as st
from sqlglot import exp

from SrcTgtArtifact import excel_bytes
from SrcTgtConnection import connection_frame, connection_label, runtime_connection_values, selectable_connections, validate_mapping_connections
from SrcTgtDataType import redshift_type
from SrcTgtLoadState import INCREMENT_METHODS, SYSTEM_COLUMN_FORMATS, normalize_name_array, normalize_parallel, transition_plan


TABLE_FIELDS = [
    "MPG_ID", "PRJ_CD", "SBJ_AREA_CD", "TGT_CONN_ID", "TGT_SCH_NM", "TGT_TBL_NM", "TGT_TBL_CMT",
    "SRC_CONN_ID", "SRC_SCH_NM", "SRC_TBL_NM", "LOAD_STS_CD", "SYS_COL_NM_ARR", "SYS_COL_FMT_CD", "INCR_MTHD_CD", "SRC_INCR_COL_NM_ARR", "PARL_MTHD_CD", "PARL_CND_ARR",
]

COLUMN_PARENT_FIELDS = ["MPG_ID", "PRJ_CD", "SBJ_AREA_CD", "TGT_CONN_ID", "TGT_SCH_NM", "TGT_TBL_NM", "SRC_CONN_ID", "SRC_SCH_NM", "SRC_TBL_NM"]
COLUMN_DETAIL_FIELDS = ["COL_ORD", "TGT_COL_NO", "TGT_COL_NM", "TGT_COL_CMT", "TGT_DATA_TYPE", "TGT_NULL_YN", "TGT_KEY_ROLE_CD", "COL_MPG_MTHD_CD", "TGT_EXPR", "DFLT_EXPR", "S3_COL_NM", "S3_DATA_TYPE", "SRC_EXPR", "SRC_REF_COL_NM_ARR", "SRC_COL_NO", "SRC_COL_NM", "SRC_DATA_TYPE", "SRC_NULL_YN", "SRC_KEY_ROLE_CD", "SUM_VALD_YN", "HSH_VALD_YN"]
COLUMN_FIELDS = [*COLUMN_PARENT_FIELDS, *COLUMN_DETAIL_FIELDS]

FIELD_LABELS = {
    "MPG_ID": "테이블매핑ID", "PRJ_CD": "프로젝트코드", "SBJ_AREA_CD": "주제영역코드", "SRC_CONN_ID": "원천접속ID", "SRC_SCH_NM": "원천스키마명", "SRC_TBL_NM": "원천테이블명",
    "TGT_CONN_ID": "대상접속ID", "TGT_SCH_NM": "대상스키마명", "TGT_TBL_NM": "대상테이블명", "TGT_TBL_CMT": "대상테이블설명", "LOAD_STS_CD": "적재상태", "SYS_COL_NM_ARR": "시스템컬럼명배열", "SYS_COL_FMT_CD": "시스템컬럼데이터형식", "INCR_MTHD_CD": "증분방식", "SRC_INCR_COL_NM_ARR": "원천증분컬럼명배열", "PARL_MTHD_CD": "S3병렬방식", "PARL_CND_ARR": "S3병렬조건",
    "COL_ORD": "매핑순서", "TGT_COL_NO": "대상컬럼순번", "TGT_COL_NM": "대상컬럼명", "TGT_COL_CMT": "대상컬럼설명", "TGT_DATA_TYPE": "대상데이터타입", "TGT_NULL_YN": "대상NULL허용여부", "TGT_KEY_ROLE_CD": "대상키역할코드", "COL_MPG_MTHD_CD": "컬럼매핑방식", "TGT_EXPR": "이행적용SQL식", "DFLT_EXPR": "이행기본값SQL식", "S3_COL_NM": "S3중간컬럼명", "S3_DATA_TYPE": "S3중간데이터타입", "SRC_EXPR": "이관적용SQL식", "SRC_REF_COL_NM_ARR": "원천참조컬럼명배열", "SRC_COL_NO": "원천컬럼순번", "SRC_COL_NM": "원천컬럼명", "SRC_DATA_TYPE": "원천데이터타입", "SRC_NULL_YN": "원천NULL허용여부", "SRC_KEY_ROLE_CD": "원천키역할코드", "SUM_VALD_YN": "SUM검증여부", "HSH_VALD_YN": "HASH검증여부",
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
    output["LOAD_STS_CD"] = text(output["LOAD_STS_CD"]).upper().replace("INCREMENTAL", "INCR") or "FULL"
    output["SYS_COL_FMT_CD"] = text(output["SYS_COL_FMT_CD"]).upper() or None
    output["INCR_MTHD_CD"] = text(output["INCR_MTHD_CD"]).upper() or None
    for field, label in (("SYS_COL_NM_ARR", "시스템컬럼명"), ("SRC_INCR_COL_NM_ARR", "원천증분컬럼명")):
        values = normalize_name_array(output[field], label)
        output[field] = json.dumps(values, ensure_ascii=False) if values else None
    parallel = normalize_parallel(output["PARL_MTHD_CD"], output["PARL_CND_ARR"])
    output["PARL_MTHD_CD"] = str(parallel["method"])
    output["PARL_CND_ARR"] = json.dumps(parallel["conditions"], ensure_ascii=False) if parallel["conditions"] else None
    for field in REQUIRED_TABLE_FIELDS:
        if not text(output[field]):
            raise ValueError(f"{FIELD_LABELS[field]}은(는) 필수입니다.")
        output[field] = text(output[field])
    if output["LOAD_STS_CD"] not in {"FULL", "INCR"}:
        raise ValueError("적재 상태는 FULL 또는 INCR 중 하나여야 합니다.")
    if output["LOAD_STS_CD"] == "INCR":
        normalize_name_array(output["SYS_COL_NM_ARR"], "시스템컬럼명", required=True)
        if output["SYS_COL_FMT_CD"] not in SYSTEM_COLUMN_FORMATS:
            raise ValueError("시스템컬럼 데이터 형식을 선택하십시오.")
        if output["INCR_MTHD_CD"] not in INCREMENT_METHODS:
            raise ValueError("증분 방식을 선택하십시오.")
        normalize_name_array(output["SRC_INCR_COL_NM_ARR"], "원천증분컬럼명", required=True)
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
        for field in ("SRC_COL_NM", "SRC_DATA_TYPE", "SRC_KEY_ROLE_CD", "TGT_COL_NM", "TGT_COL_CMT", "TGT_KEY_ROLE_CD", "S3_COL_NM", "S3_DATA_TYPE", "SRC_EXPR", "TGT_EXPR", "DFLT_EXPR"):
            row[field] = text(row[field]) or None
        row["COL_MPG_MTHD_CD"] = text(row["COL_MPG_MTHD_CD"]).upper() or "MOVE"
        if row["COL_MPG_MTHD_CD"] not in {"MOVE", "CONST", "NULL", "EXPR"}:
            raise ValueError(f"컬럼매핑 {number}행의 컬럼매핑방식은 MOVE, CONST, NULL, EXPR 중 하나여야 합니다.")
        references = normalize_name_array(row["SRC_REF_COL_NM_ARR"], "원천참조컬럼명")
        if row["COL_MPG_MTHD_CD"] == "MOVE":
            if not row["SRC_COL_NM"]:
                raise ValueError(f"컬럼매핑 {number}행의 MOVE 방식에는 원천컬럼명이 필요합니다.")
            references = list(dict.fromkeys([row["SRC_COL_NM"], *references]))
        elif row["COL_MPG_MTHD_CD"] == "CONST" and not row["TGT_EXPR"]:
            raise ValueError(f"컬럼매핑 {number}행의 CONST 방식에는 이행적용SQL식이 필요합니다.")
        elif row["COL_MPG_MTHD_CD"] == "EXPR" and not row["SRC_EXPR"] and not row["TGT_EXPR"]:
            raise ValueError(f"컬럼매핑 {number}행의 EXPR 방식에는 이관 또는 이행 적용SQL식이 필요합니다.")
        if row["COL_MPG_MTHD_CD"] in {"CONST", "NULL"} and references:
            raise ValueError(f"컬럼매핑 {number}행의 {row['COL_MPG_MTHD_CD']} 방식에는 원천참조컬럼명을 입력할 수 없습니다.")
        if row["COL_MPG_MTHD_CD"] == "NULL" and (row["SRC_EXPR"] or row["TGT_EXPR"]):
            raise ValueError(f"컬럼매핑 {number}행의 NULL 방식에는 이관·이행 적용SQL식을 입력할 수 없습니다.")
        requires_stage = row["COL_MPG_MTHD_CD"] not in {"CONST", "NULL"} and not row["TGT_EXPR"]
        if requires_stage and not row["S3_COL_NM"]:
            row["S3_COL_NM"] = row["SRC_COL_NM"] or row["TGT_COL_NM"]
        if row["S3_COL_NM"] and not row["S3_DATA_TYPE"]:
            row["S3_DATA_TYPE"] = row["SRC_DATA_TYPE"] or row["TGT_DATA_TYPE"]
        if row["COL_MPG_MTHD_CD"] in {"CONST", "NULL"} and row["S3_COL_NM"]:
            raise ValueError(f"컬럼매핑 {number}행의 {row['COL_MPG_MTHD_CD']} 방식에는 S3중간컬럼명을 입력할 수 없습니다.")
        row["SRC_REF_COL_NM_ARR"] = json.dumps(references, ensure_ascii=False) if references else None
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


def validate_tables(frame: pd.DataFrame) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    keys: set[tuple[str, ...]] = set()
    for number, source in enumerate(frame.to_dict(orient="records"), start=2):
        row = defaults(source)
        row["MPG_ID"] = integer(source.get("MPG_ID"), "MPG_ID")
        key = natural_key(row)
        if key in keys:
            raise ValueError(f"테이블매핑 {number}행의 원천·대상 식별값이 중복됩니다.")
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


def record_mapping_change(cursor: Any, schema_name: str, qualified: Callable[[str, str], str], mapping_id_value: int, division: str, reason: str, after_value: dict[str, object], before_value: dict[str, object] | None = None) -> None:
    cursor.execute(f"SELECT meta_ver_no FROM {qualified(schema_name, 'tb_mig_tbl_mpg')} WHERE mpg_id = %s", (mapping_id_value,))
    version_row = cursor.fetchone()
    if version_row is None:
        raise ValueError(f"테이블매핑을 찾을 수 없습니다: {mapping_id_value}")
    cursor.execute(
        f"INSERT INTO {qualified(schema_name, 'tb_mig_mpg_chg_hist')} (mpg_id, meta_ver_no, chg_dvsn_cd, chg_rsn, bf_val, af_val) VALUES (%s, %s, %s, %s, %s, %s)",
        (mapping_id_value, int(version_row[0]), division, reason, json.dumps(before_value, ensure_ascii=False, default=str, sort_keys=True) if before_value is not None else None, json.dumps(after_value, ensure_ascii=False, default=str, sort_keys=True)),
    )


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


def save_bundle(connect: Callable[[dict[str, Any]], Any], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], tables: list[dict[str, object]], columns: list[dict[str, object]], replace_columns: bool) -> tuple[int, int]:
    table_name = qualified(schema_name, "tb_mig_col_mpg")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            subject_table = qualified(schema_name, "tb_mig_sbj_area")
            for subject_area in sorted({text(row["SBJ_AREA_CD"]) for row in tables}):
                cursor.execute(f"SELECT 1 FROM {subject_table} WHERE sbj_area_cd = %s AND active_yn = TRUE", (subject_area,))
                if cursor.fetchone() is None:
                    raise ValueError(f"사용 중인 주제영역을 찾을 수 없습니다: {subject_area}")
            for row in tables:
                validate_mapping_connections(cursor, schema_name, qualified, row["SRC_CONN_ID"], row["TGT_CONN_ID"])
            uploaded_ids = {natural_key(row): upsert_table(cursor, schema_name, qualified, row) for row in tables}
            for row in tables:
                record_mapping_change(cursor, schema_name, qualified, uploaded_ids[natural_key(row)], "TBL_MPG", "테이블매핑 저장", row)
            uploaded_scopes = {uploaded_ids[natural_key(row)]: (text(row["PRJ_CD"]), text(row["SBJ_AREA_CD"])) for row in tables}
            mapped_columns: list[tuple[int, dict[str, object]]] = []
            seen_orders: set[tuple[int, int]] = set()
            for row in columns:
                map_id = resolve_column_mapping_id(cursor, schema_name, qualified, row, uploaded_ids)
                scope = uploaded_scopes.get(map_id)
                if scope is None:
                    scope = mapping_scope(cursor, schema_name, qualified, map_id)
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
                    f'''INSERT INTO {table_name} (mpg_id, col_ord, tgt_col_no, tgt_col_nm, tgt_col_cmt, tgt_data_type, tgt_null_yn, tgt_key_role_cd, col_mpg_mthd_cd, tgt_expr, dflt_expr, s3_col_nm, s3_data_type, src_expr, src_col_no, src_col_nm, src_data_type, src_null_yn, src_key_role_cd, src_ref_col_nm_arr, sum_vald_yn, hsh_vald_yn, active_yn) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)''',
                    (map_id, row["COL_ORD"], row["TGT_COL_NO"], row["TGT_COL_NM"], row["TGT_COL_CMT"], row["TGT_DATA_TYPE"], row["TGT_NULL_YN"], row["TGT_KEY_ROLE_CD"], row["COL_MPG_MTHD_CD"], row["TGT_EXPR"], row["DFLT_EXPR"], row["S3_COL_NM"], row["S3_DATA_TYPE"], row["SRC_EXPR"], row["SRC_COL_NO"], row["SRC_COL_NM"], row["SRC_DATA_TYPE"], row["SRC_NULL_YN"], row["SRC_KEY_ROLE_CD"], row["SRC_REF_COL_NM_ARR"], row["SUM_VALD_YN"], row["HSH_VALD_YN"]),
                )
                record_mapping_change(cursor, schema_name, qualified, map_id, "COL_MPG", "컬럼매핑 저장", row)
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
    query = f'''SELECT col_ord AS "COL_ORD", tgt_col_no AS "TGT_COL_NO", tgt_col_nm AS "TGT_COL_NM", tgt_col_cmt AS "TGT_COL_CMT", tgt_data_type AS "TGT_DATA_TYPE", tgt_null_yn AS "TGT_NULL_YN", tgt_key_role_cd AS "TGT_KEY_ROLE_CD", col_mpg_mthd_cd AS "COL_MPG_MTHD_CD", tgt_expr AS "TGT_EXPR", dflt_expr AS "DFLT_EXPR", s3_col_nm AS "S3_COL_NM", s3_data_type AS "S3_DATA_TYPE", src_expr AS "SRC_EXPR", src_ref_col_nm_arr AS "SRC_REF_COL_NM_ARR", src_col_no AS "SRC_COL_NO", src_col_nm AS "SRC_COL_NM", src_data_type AS "SRC_DATA_TYPE", src_null_yn AS "SRC_NULL_YN", src_key_role_cd AS "SRC_KEY_ROLE_CD", sum_vald_yn AS "SUM_VALD_YN", hsh_vald_yn AS "HSH_VALD_YN"
                  FROM {qualified(schema_name, "tb_mig_col_mpg")}
                 WHERE mpg_id = %s AND active_yn = TRUE
                 ORDER BY col_ord'''
    return query_frame(values, query, (mapping_id,))


def form_value(row: pd.Series | None, field: str, default: str = "") -> str:
    return text(row[field.lower()]) if row is not None and field.lower() in row.index else default


def source_snapshots(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str]) -> pd.DataFrame:
    query = f'''SELECT src_conn_id AS "SRC_CONN_ID", std_dt AS "STD_DT", src_sch_nm AS "OWNER", src_tbl_nm AS "TBL", MAX(src_tbl_cmt) AS "ENTITY"
                  FROM {qualified(schema_name, "tb_mig_src_layout")}
                 GROUP BY src_conn_id, std_dt, src_sch_nm, src_tbl_nm
                 ORDER BY std_dt DESC, src_conn_id, src_sch_nm, src_tbl_nm'''
    return query_frame(values, query)


def source_columns(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], source_connection_id: str, standard_date: str, owner: str, table: str, character_multiple: object = 3) -> pd.DataFrame:
    query = f'''SELECT src_col_no AS "COL_ORD", src_col_no AS "SRC_COL_NO", src_col_nm AS "SRC_COL_NM", src_col_cmt AS "TGT_COL_CMT", src_data_type AS "SRC_DATA_TYPE", src_data_len AS "SRC_DATA_LEN",
                      src_null_yn AS "SRC_NULL_YN", CASE WHEN src_pk_yn THEN 'PK' ELSE NULL END AS "SRC_KEY_ROLE_CD"
                  FROM {qualified(schema_name, "tb_mig_src_layout")}
                 WHERE src_conn_id = %s AND std_dt = %s AND src_sch_nm = %s AND src_tbl_nm = %s
                 ORDER BY src_col_no'''
    source = query_frame(values, query, (source_connection_id, standard_date, owner, table))
    if source.empty:
        raise ValueError("선택한 원천 레이아웃의 컬럼을 찾을 수 없습니다.")
    result = pd.DataFrame(columns=COLUMN_DETAIL_FIELDS)
    for column in source.columns:
        result[column] = source[column]
    result["TGT_COL_NO"] = source["COL_ORD"]
    result["TGT_COL_NM"] = source["SRC_COL_NM"]
    source_lengths = source["SRC_DATA_LEN"] if "SRC_DATA_LEN" in source.columns else pd.Series([None] * len(source), index=source.index)
    result["TGT_DATA_TYPE"] = [redshift_type(data_type, data_length, character_multiple) for data_type, data_length in zip(source["SRC_DATA_TYPE"], source_lengths)]
    result["TGT_NULL_YN"] = source["SRC_NULL_YN"]
    result["TGT_KEY_ROLE_CD"] = source["SRC_KEY_ROLE_CD"]
    result["COL_MPG_MTHD_CD"] = "MOVE"
    result["TGT_EXPR"] = None
    result["DFLT_EXPR"] = None
    result["S3_COL_NM"] = source["SRC_COL_NM"]
    result["S3_DATA_TYPE"] = source["SRC_DATA_TYPE"]
    result["SRC_EXPR"] = None
    result["SRC_REF_COL_NM_ARR"] = source["SRC_COL_NM"].map(lambda value: json.dumps([text(value)], ensure_ascii=False))
    result["SUM_VALD_YN"] = False
    result["HSH_VALD_YN"] = False
    return result.loc[:, COLUMN_DETAIL_FIELDS]


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


def render_single(maps: pd.DataFrame, values: dict[str, Any], schema_name: str, query_frame: Callable[..., pd.DataFrame], connect: Callable[[dict[str, Any]], Any], qualified: Callable[[str, str], str]) -> None:
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
            snapshots = source_snapshots(query_frame, values, schema_name, qualified)
            snapshots = snapshots.loc[snapshots.SRC_CONN_ID.map(text).str.upper().isin(source_connections.conn_id.map(text).str.upper())].copy()
            if snapshots.empty:
                raise ValueError("사용 중인 원천 접속정보의 레이아웃 적재 이력이 없습니다. 먼저 원천 레이아웃에서 기준일을 적재하십시오.")
            snapshot_options = snapshots[["SRC_CONN_ID", "STD_DT", "OWNER", "TBL"]].astype(str).agg(" | ".join, axis=1).tolist()
            selected_snapshot = st.selectbox("원천 레이아웃", snapshot_options, key="new_mapping_source")
            source_snapshot = snapshots.iloc[snapshot_options.index(selected_snapshot)]
            source_connection = source_connections.loc[source_connections.conn_id.map(text).str.upper().eq(text(source_snapshot.SRC_CONN_ID).upper())].iloc[0]
            character_multiple = int(source_connection.get("char_len_mul", 3) or 3)
            existing = source_columns(query_frame, values, schema_name, qualified, text(source_snapshot.SRC_CONN_ID), text(source_snapshot.STD_DT), text(source_snapshot.OWNER), text(source_snapshot.TBL), character_multiple)
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
            target_default = form_value(current, "TGT_CONN_ID", text(target_connections.iloc[0].conn_id)).upper()
            tgt_conn_id = st.selectbox("대상접속ID", target_connections.conn_id.tolist(), index=target_connections.conn_id.tolist().index(target_default) if target_default in target_connections.conn_id.tolist() else 0, format_func=lambda value: connection_label(target_connections, value))
            tgt_sch_nm = st.text_input("대상스키마명", value=form_value(current, "TGT_SCH_NM"))
            tgt_tbl_nm = st.text_input("대상테이블명", value=form_value(current, "TGT_TBL_NM"))
            tgt_tbl_cmt = st.text_area("대상테이블설명", value=text(source_snapshot.ENTITY) if source_snapshot is not None else form_value(current, "TGT_TBL_CMT"), height=80)
            if source_snapshot is not None:
                src_conn_id = text(source_snapshot.SRC_CONN_ID).upper()
                st.text_input("원천접속ID", value=connection_label(source_connections, src_conn_id), disabled=True)
            else:
                source_default = form_value(current, "SRC_CONN_ID", text(source_connections.iloc[0].conn_id)).upper()
                src_conn_id = st.selectbox("원천접속ID", source_connections.conn_id.tolist(), index=source_connections.conn_id.tolist().index(source_default) if source_default in source_connections.conn_id.tolist() else 0, format_func=lambda value: connection_label(source_connections, value))
            src_sch_nm = st.text_input("원천스키마명", value=text(source_snapshot.OWNER) if source_snapshot is not None else form_value(current, "SRC_SCH_NM"), disabled=True)
            src_tbl_nm = st.text_input("원천테이블명", value=text(source_snapshot.TBL) if source_snapshot is not None else form_value(current, "SRC_TBL_NM"), disabled=True)
        with execution:
            st.selectbox("적재 상태", [form_value(current, "LOAD_STS_CD", "FULL") or "FULL"], disabled=True)
            system_columns = st.text_input("시스템 컬럼명", value=form_value(current, "SYS_COL_NM_ARR"), placeholder='["생성일시", "수정일시"]')
            format_options = ["", *sorted(SYSTEM_COLUMN_FORMATS)]
            system_format = st.selectbox("시스템 컬럼 형식", format_options, index=format_options.index(form_value(current, "SYS_COL_FMT_CD")) if form_value(current, "SYS_COL_FMT_CD") in format_options else 0)
            increment_options = ["", "PK_MERGE", "APPEND"]
            increment_method = st.selectbox("증분 방식", increment_options, index=increment_options.index(form_value(current, "INCR_MTHD_CD")) if form_value(current, "INCR_MTHD_CD") in increment_options else 0, format_func=lambda value: {"": "선택", "PK_MERGE": "PK 기준 DELETE·INSERT", "APPEND": "증분컬럼 기준 DELETE·INSERT"}[value])
            increment_columns = st.text_input("원천 증분 컬럼명", value=form_value(current, "SRC_INCR_COL_NM_ARR"), placeholder='["PK1", "PK2"]')
            parl_mthd_cd = st.selectbox("S3 병렬 방식", ["NONE", "WHERE"], index=["NONE", "WHERE"].index(form_value(current, "PARL_MTHD_CD", "NONE").upper()) if form_value(current, "PARL_MTHD_CD", "NONE").upper() in {"NONE", "WHERE"} else 0)
            parl_cnd_arr = st.text_area("S3 병렬 조건", value=form_value(current, "PARL_CND_ARR"), disabled=parl_mthd_cd != "WHERE", placeholder='["abc_dt BETWEEN \'19000101\' AND \'20001231\'", "abc_dt BETWEEN \'20010101\' AND \'20101231\'"]')
        st.markdown("##### 컬럼 매핑")
        edited = st.data_editor(existing, num_rows="dynamic", hide_index=True, key=f"single_column_editor_{selected}", column_config={"COL_ORD": st.column_config.NumberColumn("매핑순서", min_value=1, step=1), "TGT_COL_NO": st.column_config.NumberColumn("대상컬럼순번", min_value=1, step=1), "TGT_COL_NM": "대상컬럼명", "TGT_COL_CMT": "대상컬럼설명", "TGT_DATA_TYPE": "대상데이터타입", "TGT_NULL_YN": st.column_config.CheckboxColumn("대상NULL허용여부"), "TGT_KEY_ROLE_CD": "대상키역할코드", "COL_MPG_MTHD_CD": st.column_config.SelectboxColumn("컬럼매핑방식", options=["MOVE", "CONST", "NULL", "EXPR"], required=True), "TGT_EXPR": "이행적용SQL식", "DFLT_EXPR": "이행기본값SQL식", "S3_COL_NM": "S3중간컬럼명", "S3_DATA_TYPE": "S3중간데이터타입", "SRC_EXPR": "이관적용SQL식", "SRC_REF_COL_NM_ARR": "원천참조컬럼명배열", "SRC_COL_NO": st.column_config.NumberColumn("원천컬럼순번", min_value=1, step=1), "SRC_COL_NM": "원천컬럼명", "SRC_DATA_TYPE": "원천데이터타입", "SRC_NULL_YN": st.column_config.CheckboxColumn("원천NULL허용여부"), "SRC_KEY_ROLE_CD": "원천키역할코드", "SUM_VALD_YN": st.column_config.CheckboxColumn("SUM검증여부"), "HSH_VALD_YN": st.column_config.CheckboxColumn("HASH검증여부")})
        automatic = st.form_submit_button("대상 구조 자동 반영", icon=":material/auto_fix_high:")
        generated = st.form_submit_button("SQL 생성", icon=":material/code:")
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
    if submitted or generated:
        try:
            source = {} if current is None else {field: current[field.lower()] for field in TABLE_FIELDS if field.lower() in current.index}
            source.update({"MPG_ID": None if current is None else int(selected), "PRJ_CD": prj_cd, "SBJ_AREA_CD": sbj_area_cd, "SRC_CONN_ID": src_conn_id, "SRC_SCH_NM": src_sch_nm, "SRC_TBL_NM": src_tbl_nm, "TGT_CONN_ID": tgt_conn_id, "TGT_SCH_NM": tgt_sch_nm, "TGT_TBL_NM": tgt_tbl_nm, "TGT_TBL_CMT": tgt_tbl_cmt, "LOAD_STS_CD": "FULL" if current is None else form_value(current, "LOAD_STS_CD", "FULL"), "SYS_COL_NM_ARR": system_columns, "SYS_COL_FMT_CD": system_format, "INCR_MTHD_CD": increment_method, "SRC_INCR_COL_NM_ARR": increment_columns, "PARL_MTHD_CD": parl_mthd_cd, "PARL_CND_ARR": parl_cnd_arr})
            table = defaults(source)
            table["MPG_ID"] = None if current is None else int(selected)
            column_input = edited.copy()
            for field in NATURAL_FIELDS:
                column_input[field] = table[field]
            column_input["MPG_ID"] = table["MPG_ID"]
            columns = normalized_columns(column_input)
            save_bundle(connect, values, schema_name, qualified, [table], columns, True)
            if generated:
                with connect(values) as connection:
                    with connection.cursor() as cursor:
                        saved_id = mapping_id(cursor, schema_name, qualified, table)
                if saved_id is None:
                    raise ValueError("저장한 테이블매핑 ID를 확인할 수 없습니다.")
                source_sql, target_sql = sql_templates(pd.Series({field.lower(): value for field, value in table.items()}), pd.DataFrame(columns))
                save_sql_overrides(connect, values, schema_name, qualified, int(saved_id), source_sql, target_sql, columns)
            st.session_state.pop(automatic_key, None)
            if generated:
                st.success("이관·이행 SQL을 생성하고 이력과 함께 저장했습니다.", icon=":material/check_circle:")
            st.rerun()
        except Exception as error:
            st.error(f"매핑 저장 실패: {error}", icon=":material/error:")


def render_upload(values: dict[str, Any], schema_name: str, connect: Callable[[dict[str, Any]], Any], qualified: Callable[[str, str], str]) -> None:
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
            tables = validate_tables(table_frame)
            columns = normalized_columns(column_frame)
            saved_tables, saved_columns = save_bundle(connect, values, schema_name, qualified, tables, columns, replace_columns)
            st.success(f"테이블매핑 {saved_tables:,}건, 컬럼매핑 {saved_columns:,}건을 반영했습니다.", icon=":material/check_circle:")
        except Exception as error:
            st.error(f"일괄 반영 실패: {error}", icon=":material/error:")


def sql_identifier(value: object) -> str:
    name = text(value)
    if not name or "\x00" in name:
        raise ValueError("SQL 식별자를 확인하십시오.")
    return '"' + name.replace('"', '""') + '"'


def sql_value(row: dict[str, object]) -> str:
    method = text(row.get("COL_MPG_MTHD_CD")).upper() or "MOVE"
    expression = text(row.get("TGT_EXPR"))
    stage_name = text(row.get("S3_COL_NM"))
    if expression:
        value = expression
    elif method == "NULL":
        value = "CAST(NULL AS " + redshift_type(row.get("TGT_DATA_TYPE")) + ")"
    elif stage_name:
        value = "S." + sql_identifier(stage_name)
    elif method in {"CONST", "EXPR"}:
        raise ValueError(f"{method} 컬럼매핑에는 이행적용SQL식 또는 S3중간컬럼명이 필요합니다.")
    elif method == "MOVE":
        raise ValueError("MOVE 컬럼매핑에는 S3중간컬럼명이 필요합니다.")
    else:
        raise ValueError("컬럼매핑방식을 확인하십시오.")
    default = text(row.get("DFLT_EXPR"))
    return f"COALESCE({value}, {default})" if default else value


def sql_references(row: dict[str, object]) -> list[str]:
    if text(row.get("COL_MPG_MTHD_CD")).upper() in {"CONST", "NULL"}:
        return []
    values = normalize_name_array(row.get("SRC_REF_COL_NM_ARR"), "원천참조컬럼명")
    source_name = text(row.get("SRC_COL_NM"))
    return list(dict.fromkeys([source_name, *values])) if source_name else values


def source_projection(row: dict[str, object]) -> str:
    method = text(row.get("COL_MPG_MTHD_CD")).upper() or "MOVE"
    stage_name = text(row.get("S3_COL_NM"))
    if not stage_name:
        if method in {"CONST", "NULL"}:
            return ""
        raise ValueError(f"{method} 컬럼매핑에는 S3중간컬럼명이 필요합니다.")
    expression = text(row.get("SRC_EXPR"))
    source_name = text(row.get("SRC_COL_NM"))
    if not expression:
        if not source_name:
            raise ValueError(f"S3중간컬럼 {stage_name}의 이관 SQL식 또는 원천컬럼명이 필요합니다.")
        expression = "S." + sql_identifier(source_name)
    return expression + " AS " + sql_identifier(stage_name)


def sql_templates(table: pd.Series, columns: pd.DataFrame) -> tuple[str, str]:
    if columns.empty:
        raise ValueError("컬럼매핑을 한 건 이상 입력하십시오.")
    rows = columns.to_dict(orient="records")
    source_table = sql_identifier(table.src_sch_nm) + "." + sql_identifier(table.src_tbl_nm)
    source_values = ", ".join(value for value in (source_projection(row) for row in rows) if value) or '1 AS "MIG_DUMMY"'
    source_sql = "SELECT " + source_values + " FROM " + source_table + " AS S WHERE __SRC_WHERE_CND__"
    target = sql_identifier(table.tgt_sch_nm) + "." + sql_identifier(table.tgt_tbl_nm)
    target_columns = ", ".join(sql_identifier(row["TGT_COL_NM"]) for row in rows)
    insert_sql = "INSERT INTO " + target + " (" + target_columns + ") SELECT " + ", ".join(sql_value(row) for row in rows) + " FROM __MIG_STAGE__ AS S"
    if text(table.load_sts_cd).upper() == "FULL":
        return source_sql, "TRUNCATE TABLE " + target + ";\n" + insert_sql + ";"
    source_keys = normalize_name_array(table.src_incr_col_nm_arr, "원천증분컬럼명", required=True)
    by_source = {text(row["SRC_COL_NM"]).upper(): row for row in rows}
    target_keys: list[str] = []
    staged_keys: list[str] = []
    for source_key in source_keys:
        row = by_source.get(source_key.upper())
        if row is None:
            raise ValueError(f"원천 증분 컬럼의 대상 컬럼매핑이 없습니다: {source_key}")
        target_key = text(row["TGT_COL_NM"])
        target_keys.append(target_key)
        staged_keys.append(sql_value(row) + " AS " + sql_identifier(target_key))
    delete_sql = "DELETE FROM " + target + " AS T USING (SELECT DISTINCT " + ", ".join(staged_keys) + " FROM __MIG_STAGE__ AS S) AS S WHERE " + " AND ".join("T." + sql_identifier(value) + " = S." + sql_identifier(value) for value in target_keys)
    return source_sql, delete_sql + ";\n" + insert_sql + ";"


def sql_statements(value: str, label: str) -> list[exp.Expression]:
    try:
        statements = sqlglot.parse(value, read="redshift")
    except sqlglot.errors.ParseError as error:
        raise ValueError(f"{label} 문법을 확인하십시오: {error}") from error
    if not statements:
        raise ValueError(f"{label}이 비어 있습니다.")
    return statements


def select_count(statement: exp.Expression, label: str) -> int:
    select = statement if isinstance(statement, exp.Select) else statement.find(exp.Select)
    if select is None:
        raise ValueError(f"{label}에는 SELECT 문이 필요합니다.")
    if any(isinstance(item, exp.Star) or item.find(exp.Star) is not None for item in select.expressions):
        raise ValueError(f"{label}에는 * 대신 명시 컬럼을 사용하십시오.")
    return len(select.expressions)


def validate_sql_pair(source_sql: object, target_sql: object, columns: pd.DataFrame) -> None:
    source_value = text(source_sql)
    target_value = text(target_sql)
    if not source_value or not target_value:
        raise ValueError("이관 SQL과 이행 SQL을 모두 저장하십시오.")
    if "__SRC_WHERE_CND__" not in source_value:
        raise ValueError("이관 SQL에는 __SRC_WHERE_CND__ 치환값이 필요합니다.")
    if "__MIG_STAGE__" not in target_value:
        raise ValueError("이행 SQL에는 __MIG_STAGE__ 치환값이 필요합니다.")
    source_statements = sql_statements(source_value, "이관 SQL")
    if len(source_statements) != 1:
        raise ValueError("이관 SQL은 SELECT 문 한 개여야 합니다.")
    expected_source = max(sum(bool(text(row.get("S3_COL_NM"))) for row in columns.to_dict(orient="records")), 1)
    actual_source = select_count(source_statements[0], "이관 SQL")
    if actual_source != expected_source:
        raise ValueError(f"이관 SQL SELECT 컬럼수({actual_source})와 S3중간컬럼수({expected_source})가 다릅니다.")
    inserts = [statement for statement in sql_statements(target_value, "이행 SQL") if isinstance(statement, exp.Insert)]
    if len(inserts) != 1:
        raise ValueError("이행 SQL에는 INSERT 문 한 개가 필요합니다.")
    insert = inserts[0]
    if not isinstance(insert.this, exp.Schema):
        raise ValueError("이행 SQL INSERT에는 대상 컬럼 목록이 필요합니다.")
    expected_target = len(columns)
    actual_target = len(insert.this.expressions)
    actual_select = select_count(insert.expression, "이행 SQL INSERT")
    if actual_target != expected_target or actual_select != expected_target:
        raise ValueError(f"이행 SQL 대상·SELECT 컬럼수({actual_target}/{actual_select})와 컬럼매핑수({expected_target})가 다릅니다.")


def read_sql_overrides(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], mapping_id_value: int) -> pd.Series:
    frame = query_frame(values, f"SELECT src_ext_sql, tgt_load_sql FROM {qualified(schema_name, 'tb_mig_tbl_mpg')} WHERE mpg_id = %s AND active_yn = TRUE", (mapping_id_value,))
    if len(frame) != 1:
        raise ValueError("활성 테이블매핑을 찾을 수 없습니다.")
    return frame.iloc[0]


def save_sql_overrides(connect: Callable[[dict[str, Any]], Any], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], mapping_id_value: int, source_sql: object, target_sql: object, columns: pd.DataFrame) -> None:
    source_value = text(source_sql) or None
    target_value = text(target_sql) or None
    validate_sql_pair(source_value, target_value, columns)
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT src_ext_sql, tgt_load_sql FROM {qualified(schema_name, 'tb_mig_tbl_mpg')} WHERE mpg_id = %s AND active_yn = TRUE", (mapping_id_value,))
            current = cursor.fetchone()
            if current is None:
                raise ValueError("활성 테이블매핑을 찾을 수 없습니다.")
            before_value = {"SRC_EXT_SQL": text(current[0]) or None, "TGT_LOAD_SQL": text(current[1]) or None}
            after_value = {"SRC_EXT_SQL": source_value, "TGT_LOAD_SQL": target_value}
            cursor.execute(f"UPDATE {qualified(schema_name, 'tb_mig_tbl_mpg')} SET src_ext_sql = %s, tgt_load_sql = %s, meta_ver_no = meta_ver_no + 1, upd_dtm = GETDATE() WHERE mpg_id = %s AND active_yn = TRUE", (source_value, target_value, mapping_id_value))
            if cursor.rowcount != 1:
                raise ValueError("활성 테이블매핑을 찾을 수 없습니다.")
            record_mapping_change(cursor, schema_name, qualified, mapping_id_value, "SQL_MPG", "실행SQL 저장", after_value, before_value)
        connection.commit()


def sql_history(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], mapping_id_value: int) -> pd.DataFrame:
    return query_frame(values, f'''SELECT mpg_chg_hist_id AS "이력ID", meta_ver_no AS "메타버전", chg_rsn AS "변경사유", bf_val AS "이전SQL", af_val AS "변경SQL", crt_dtm AS "변경일시"
                                   FROM {qualified(schema_name, 'tb_mig_mpg_chg_hist')}
                                  WHERE mpg_id = %s AND chg_dvsn_cd = 'SQL_MPG'
                                  ORDER BY mpg_chg_hist_id DESC''', (mapping_id_value,))


def restore_sql_history(connect: Callable[[dict[str, Any]], Any], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], mapping_id_value: int, history_id: int) -> None:
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT src_ext_sql, tgt_load_sql FROM {qualified(schema_name, 'tb_mig_tbl_mpg')} WHERE mpg_id = %s AND active_yn = TRUE", (mapping_id_value,))
            current = cursor.fetchone()
            cursor.execute(f"SELECT af_val FROM {qualified(schema_name, 'tb_mig_mpg_chg_hist')} WHERE mpg_chg_hist_id = %s AND mpg_id = %s AND chg_dvsn_cd = 'SQL_MPG'", (history_id, mapping_id_value))
            saved = cursor.fetchone()
            if current is None or saved is None:
                raise ValueError("복원할 SQL 이력을 찾을 수 없습니다.")
            try:
                restored = json.loads(text(saved[0]) or "{}")
            except json.JSONDecodeError as error:
                raise ValueError("복원 SQL 이력 형식이 올바르지 않습니다.") from error
            source_value = text(restored.get("SRC_EXT_SQL")) or None
            target_value = text(restored.get("TGT_LOAD_SQL")) or None
            before_value = {"SRC_EXT_SQL": text(current[0]) or None, "TGT_LOAD_SQL": text(current[1]) or None}
            after_value = {"SRC_EXT_SQL": source_value, "TGT_LOAD_SQL": target_value}
            cursor.execute(f"UPDATE {qualified(schema_name, 'tb_mig_tbl_mpg')} SET src_ext_sql = %s, tgt_load_sql = %s, meta_ver_no = meta_ver_no + 1, upd_dtm = GETDATE() WHERE mpg_id = %s AND active_yn = TRUE", (source_value, target_value, mapping_id_value))
            record_mapping_change(cursor, schema_name, qualified, mapping_id_value, "SQL_MPG", f"실행SQL 이력 {history_id} 복원", after_value, before_value)
        connection.commit()


def render_sql_editor(maps: pd.DataFrame, values: dict[str, Any], schema_name: str, query_frame: Callable[..., pd.DataFrame], connect: Callable[[dict[str, Any]], Any], qualified: Callable[[str, str], str]) -> None:
    if maps.empty:
        st.info("SQL을 생성할 테이블매핑이 없습니다.", icon=":material/info:")
        return
    labels = {int(row.mpg_id): f"{int(row.mpg_id)} · {text(row.tgt_sch_nm)}.{text(row.tgt_tbl_nm)} ← {text(row.src_sch_nm)}.{text(row.src_tbl_nm)}" for row in maps.itertuples(index=False)}
    selected = st.selectbox("테이블매핑", list(labels), format_func=lambda value: labels[value], key="sql_mapping_id")
    table = maps.loc[maps.mpg_id.eq(selected)].iloc[0]
    try:
        columns = existing_columns(query_frame, values, schema_name, qualified, int(selected))
        source_auto, target_auto = sql_templates(table, columns)
        saved = read_sql_overrides(query_frame, values, schema_name, qualified, int(selected))
    except Exception as error:
        st.error(f"SQL 생성 실패: {error}", icon=":material/error:")
        return
    editor_tab, history_tab = st.tabs(["이관·이행 SQL", "SQL 이력"])
    with editor_tab:
        source_mode = st.segmented_control("SRC→S3 이관 SQL", ["자동", "수정"], default="수정" if text(saved.src_ext_sql) else "자동", key=f"source_sql_mode_{selected}")
        if source_mode == "수정":
            source_sql = st.text_area("SRC→S3 이관 SQL", value=text(saved.src_ext_sql) or source_auto, height=260, key=f"source_sql_{selected}")
        else:
            source_sql = source_auto
            st.code(source_auto, language="sql")
        target_mode = st.segmented_control("S3→TGT 이행 SQL", ["자동", "수정"], default="수정" if text(saved.tgt_load_sql) else "자동", key=f"target_sql_mode_{selected}")
        if target_mode == "수정":
            target_sql = st.text_area("S3→TGT 이행 SQL", value=text(saved.tgt_load_sql) or target_auto, height=360, key=f"target_sql_{selected}")
        else:
            target_sql = target_auto
            st.code(target_auto, language="sql")
        if st.button("SQL 저장", type="primary", icon=":material/save:", key=f"save_sql_{selected}"):
            try:
                save_sql_overrides(connect, values, schema_name, qualified, int(selected), source_sql, target_sql, columns)
                st.success("이관·이행 SQL을 저장했습니다.", icon=":material/check_circle:")
                st.rerun()
            except Exception as error:
                st.error(f"실행 SQL 저장 실패: {error}", icon=":material/error:")
    with history_tab:
        try:
            history = sql_history(query_frame, values, schema_name, qualified, int(selected))
            if history.empty:
                st.info("저장된 SQL 이력이 없습니다.", icon=":material/info:")
                return
            st.dataframe(history[["이력ID", "메타버전", "변경사유", "변경일시"]], hide_index=True)
            history_id = st.selectbox("복원 SQL 이력", history["이력ID"].astype(int).tolist(), format_func=lambda value: f"{value} · {history.loc[history['이력ID'].eq(value)].iloc[0]['변경일시']}", key=f"restore_sql_history_{selected}")
            selected_history = history.loc[history["이력ID"].eq(history_id)].iloc[0]
            st.code(text(selected_history["변경SQL"]), language="json")
            if st.button("선택 SQL 복원", type="primary", icon=":material/restore:", key=f"restore_sql_{selected}"):
                restored = json.loads(text(selected_history["변경SQL"]) or "{}")
                validate_sql_pair(restored.get("SRC_EXT_SQL"), restored.get("TGT_LOAD_SQL"), columns)
                restore_sql_history(connect, values, schema_name, qualified, int(selected), int(history_id))
                st.success("선택 SQL 이력으로 복원했습니다.", icon=":material/check_circle:")
                st.rerun()
        except Exception as error:
            st.error(f"SQL 이력 처리 실패: {error}", icon=":material/error:")


def render_load_transition(maps: pd.DataFrame, values: dict[str, Any], schema_name: str, connect: Callable[[dict[str, Any]], Any], qualified: Callable[[str, str], str]) -> None:
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
                        cursor.execute(f"SELECT load_sts_cd, sys_col_nm_arr, sys_col_fmt_cd, incr_mthd_cd, src_incr_col_nm_arr FROM {table_name} WHERE mpg_id = %s AND active_yn = TRUE", (mapping_id_value,))
                        current = cursor.fetchone()
                        if current is None:
                            raise ValueError(f"활성 테이블매핑을 찾을 수 없습니다: {mapping_id_value}")
                        cursor.execute(f"SELECT s3_manf_id FROM {manifest_name} WHERE mpg_id = %s AND vald_sts_cd = 'SUCCESS' ORDER BY s3_manf_id DESC LIMIT 1", (mapping_id_value,))
                        baseline = cursor.fetchone()
                        cursor.execute(f"SELECT 1 FROM {log_name} WHERE mpg_id = %s AND wrk_sts_cd = 'RUNNING' LIMIT 1", (mapping_id_value,))
                        running = cursor.fetchone() is not None
                        plan = transition_plan(current[0], target, None if baseline is None else baseline[0], running, current[1], current[2], current[3], current[4])
                        cursor.execute(f"UPDATE {table_name} SET load_sts_cd = %s, upd_dtm = GETDATE() WHERE mpg_id = %s", (plan["after"], mapping_id_value))
                        cursor.execute(f"INSERT INTO {history_name} (mpg_id, bf_load_sts_cd, af_load_sts_cd, chg_rsn) VALUES (%s, %s, %s, %s)", (mapping_id_value, plan["before"], plan["after"], reason.strip()))
                        changed += 1
                connection.commit()
            st.success(f"테이블매핑 {changed:,}건의 적재상태를 전환했습니다.", icon=":material/check_circle:")
            st.rerun()
        except Exception as error:
            st.error(f"상태 전환 실패: {error}", icon=":material/error:")


def render_mapping_workspace(maps: pd.DataFrame, values: dict[str, Any], schema_name: str, query_frame: Callable[..., pd.DataFrame], connect: Callable[[dict[str, Any]], Any], qualified: Callable[[str, str], str]) -> None:
    mapping_tab, sql_tab, transition_tab, upload_tab = st.tabs(["매핑", "SQL 생성·수정", "적재상태 전환", "일괄 업로드"])
    with mapping_tab:
        render_single(maps, values, schema_name, query_frame, connect, qualified)
    with sql_tab:
        render_sql_editor(maps, values, schema_name, query_frame, connect, qualified)
    with transition_tab:
        render_load_transition(maps, values, schema_name, connect, qualified)
    with upload_tab:
        render_upload(values, schema_name, connect, qualified)

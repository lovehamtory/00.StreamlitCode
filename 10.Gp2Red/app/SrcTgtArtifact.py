from __future__ import annotations

from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import streamlit as st


LAYOUTS: dict[str, dict[str, Any]] = {
    "TBL_DFN": {
        "name": "테이블정의서",
        "sheet": "테이블정의서",
        "items": [
            ("MPG_ID", "테이블매핑ID"), ("PRJ_CD", "프로젝트코드"), ("SBJ_AREA_CD", "주제영역코드"), ("SBJ_AREA_NM", "주제영역명"),
            ("SRC_CONN_ID", "원천접속ID"), ("SRC_SCH_NM", "원천스키마명"), ("SRC_TBL_NM", "원천테이블명"),
            ("TGT_CONN_ID", "대상접속ID"), ("TGT_SCH_NM", "대상스키마명"), ("TGT_TBL_NM", "대상테이블명"), ("TGT_DIST_STYLE", "대상분산방식"), ("TGT_DIST_KEY_COL", "대상분산키컬럼명"), ("TGT_SORT_STYLE", "대상정렬방식"), ("TGT_SORT_COLS", "대상정렬키컬럼목록"), ("TGT_ENCD_AUTO_YN", "대상자동압축여부"),
            ("LOAD_STS_CD", "이관적재상태코드"), ("INCR_BASIS_CD", "증분기준구분코드"), ("INCR_BASIS_COL_NM", "증분기준컬럼명"), ("PARL_MTHD_CD", "S3추출병렬방식코드"), ("PARL_CND_ARR", "S3추출병렬조건배열"), ("META_VER_NO", "메타데이터버전번호"),
        ],
    },
    "COL_DFN": {
        "name": "컬럼정의서",
        "sheet": "컬럼정의서",
        "items": [
            ("MPG_ID", "테이블매핑ID"), ("PRJ_CD", "프로젝트코드"), ("SBJ_AREA_CD", "주제영역코드"), ("SRC_SCH_NM", "원천스키마명"), ("SRC_TBL_NM", "원천테이블명"), ("TGT_SCH_NM", "대상스키마명"), ("TGT_TBL_NM", "대상테이블명"),
            ("COL_ORD", "매핑순서"), ("SRC_COL_NO", "원천컬럼순번"), ("SRC_COL_NM", "원천컬럼명"), ("SRC_DATA_TYPE", "원천데이터타입"), ("SRC_NULL_YN", "원천NULL허용여부"), ("SRC_KEY_ROLE_CD", "원천키역할코드"), ("TGT_COL_NO", "대상컬럼순번"), ("TGT_COL_NM", "대상컬럼명"), ("TGT_DATA_TYPE", "대상데이터타입"), ("TGT_NULL_YN", "대상NULL허용여부"), ("TGT_KEY_ROLE_CD", "대상키역할코드"), ("TRNSF_EXPR", "변환SQL식"), ("DFLT_EXPR", "기본값SQL식"), ("SUM_VALD_YN", "SUM검증여부"), ("HSH_VALD_YN", "HASH검증여부"),
        ],
    },
    "MPG_DFN": {
        "name": "매핑정의서",
        "sheet": "매핑정의서",
        "items": [
            ("MPG_ID", "테이블매핑ID"), ("PRJ_CD", "프로젝트코드"), ("SBJ_AREA_CD", "주제영역코드"), ("SBJ_AREA_NM", "주제영역명"), ("SRC_CONN_ID", "원천접속ID"), ("SRC_SCH_NM", "원천스키마명"), ("SRC_TBL_NM", "원천테이블명"), ("TGT_CONN_ID", "대상접속ID"), ("TGT_SCH_NM", "대상스키마명"), ("TGT_TBL_NM", "대상테이블명"), ("TGT_DIST_STYLE", "대상분산방식"), ("TGT_DIST_KEY_COL", "대상분산키컬럼명"), ("TGT_SORT_STYLE", "대상정렬방식"), ("TGT_SORT_COLS", "대상정렬키컬럼목록"), ("LOAD_STS_CD", "이관적재상태코드"), ("INCR_BASIS_CD", "증분기준구분코드"), ("INCR_BASIS_COL_NM", "증분기준컬럼명"), ("PARL_MTHD_CD", "S3추출병렬방식코드"), ("PARL_CND_ARR", "S3추출병렬조건배열"), ("COL_ORD", "매핑순서"), ("SRC_COL_NO", "원천컬럼순번"), ("SRC_COL_NM", "원천컬럼명"), ("SRC_DATA_TYPE", "원천데이터타입"), ("SRC_NULL_YN", "원천NULL허용여부"), ("SRC_KEY_ROLE_CD", "원천키역할코드"), ("TGT_COL_NO", "대상컬럼순번"), ("TGT_COL_NM", "대상컬럼명"), ("TGT_DATA_TYPE", "대상데이터타입"), ("TGT_NULL_YN", "대상NULL허용여부"), ("TGT_KEY_ROLE_CD", "대상키역할코드"), ("TRNSF_EXPR", "변환SQL식"), ("DFLT_EXPR", "기본값SQL식"), ("SUM_VALD_YN", "합계검증여부"), ("HSH_VALD_YN", "해시검증여부"),
        ],
    },
    "UTEST_RSLT": {
        "name": "단위테스트결과서",
        "sheet": "테이블별로그",
        "items": [
            ("WRK_DT", "작업일자"), ("DAG_NM", "DAG명"), ("DAG_RUN_ID", "DAG실행ID"), ("TASK_NM", "태스크명"), ("MPG_ID", "테이블매핑ID"), ("MANF_ID", "S3매니페스트ID"), ("SRC_TBL", "원천테이블"), ("TGT_TBL", "대상테이블"), ("WRK_STEP_CD", "작업단계코드"), ("WRK_STS_CD", "작업상태코드"), ("S3_MANF_PATH", "S3매니페스트경로"), ("LOAD_MTHD_CD", "실행방식코드"), ("INS_SCOPE_CD", "대상적재범위코드"), ("SRC_ROW_CNT", "원천처리건수"), ("TGT_ROW_CNT", "대상처리건수"), ("SRC_SIZE_BYTE", "원천처리크기바이트"), ("TGT_SIZE_BYTE", "대상처리크기바이트"), ("WRK_STT_DTM", "작업시작일시"), ("WRK_END_DTM", "작업종료일시"), ("WRK_ELPS_SEC", "실행경과초"), ("WRK_CND_VAL", "작업조건값"), ("SQL_FILE_PATH", "SQL파일경로"), ("LOG_FILE_PATH", "로그파일경로"), ("WRK_MSG", "작업메시지"),
        ],
    },
    "ITEST_RSLT": {
        "name": "통합테스트결과서",
        "sheet": "DAG별로그",
        "items": [
            ("WRK_DT", "작업일자"), ("DAG_NM", "DAG명"), ("DAG_RUN_ID", "DAG실행ID"), ("TBL_CNT", "테이블처리건수"), ("SUC_CNT", "성공건수"), ("FAIL_CNT", "실패건수"), ("STT_DTM", "작업시작일시"), ("END_DTM", "작업종료일시"), ("ELPS_SEC", "실행경과초"), ("WRK_STS_CD", "작업상태코드"),
        ],
    },
    "VALD_RSLT": {
        "name": "검증결과서 · 결과",
        "sheet": "검증결과",
        "items": [
            ("EXEC_RUN_ID", "이관실행ID"), ("DAG_NM", "DAG명"), ("DAG_RUN_ID", "DAG실행ID"), ("MPG_ID", "테이블매핑ID"), ("VALD_DVSN_CD", "검증구분코드"), ("S3_MANF_PATH", "S3매니페스트경로"), ("CNT_VALD_STS_CD", "건수검증상태"), ("SRC_CNT", "원천건수"), ("TGT_CNT", "대상건수"), ("CNT_DIFF", "건수차이"), ("SUM_VALD_STS_CD", "합계검증상태"), ("HSH_VALD_STS_CD", "해시검증상태"), ("VALD_STS_CD", "검증상태"), ("VALD_STT_DTM", "검증시작일시"), ("VALD_END_DTM", "검증종료일시"), ("VALD_ELPS_SEC", "검증경과초"), ("VALD_MSG", "검증메시지"),
        ],
    },
}

DOCUMENTS = {
    "TBL_DFN": ("테이블정의서", ("TBL_DFN",)),
    "COL_DFN": ("컬럼정의서", ("COL_DFN",)),
    "MPG_DFN": ("매핑정의서", ("MPG_DFN",)),
    "UTEST_RSLT": ("단위테스트결과서", ("UTEST_RSLT",)),
    "ITEST_RSLT": ("통합테스트결과서", ("ITEST_RSLT",)),
    "VALD_RSLT": ("검증결과서", ("VALD_RSLT",)),
}

ARTIFACT_ROOT = Path(__file__).parent.parent / "artifact"


def layout_label(code: str) -> str:
    item = LAYOUTS[code]
    return f"{item['name']} · {code}"


def read_layout(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], layout_code: str) -> pd.DataFrame:
    query = f'''SELECT artf_cd AS "ARTF_CD", item_cd AS "ITEM_CD", item_nm AS "ITEM_NM", disp_ord AS "DISP_ORD", out_yn AS "OUT_YN"
                  FROM {qualified(schema_name, "tb_mig_artf_item")}
                 WHERE artf_cd = %s
                 ORDER BY disp_ord, item_cd'''
    return query_frame(values, query, (layout_code,))


def configured_items(configured: pd.DataFrame, layout_code: str) -> pd.DataFrame:
    defaults = pd.DataFrame(LAYOUTS[layout_code]["items"], columns=["ITEM_CD", "ITEM_NM"])
    defaults["DISP_ORD"] = range(1, len(defaults) + 1)
    defaults["OUT_YN"] = True
    if configured.empty:
        return defaults
    current = configured.set_index("ITEM_CD")
    for index, row in defaults.iterrows():
        item_code = row["ITEM_CD"]
        if item_code in current.index:
            defaults.at[index, "ITEM_NM"] = current.at[item_code, "ITEM_NM"]
            defaults.at[index, "DISP_ORD"] = int(current.at[item_code, "DISP_ORD"])
            defaults.at[index, "OUT_YN"] = bool(current.at[item_code, "OUT_YN"])
    return defaults


def save_layout(connect: Callable[[dict[str, Any]], Any], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], layout_code: str, items: pd.DataFrame) -> None:
    table_name = qualified(schema_name, "tb_mig_artf_item")
    records = items.loc[:, ["ITEM_CD", "ITEM_NM", "DISP_ORD", "OUT_YN"]].copy()
    if records.ITEM_NM.astype(str).str.strip().eq("").any():
        raise ValueError("산출물 항목명은 비워 둘 수 없습니다.")
    if records.DISP_ORD.duplicated().any() or records.DISP_ORD.lt(1).any():
        raise ValueError("출력순서는 1 이상의 중복 없는 정수여야 합니다.")
    with connect(values) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM {table_name} WHERE artf_cd = %s", (layout_code,))
            for row in records.sort_values("DISP_ORD").itertuples(index=False):
                cursor.execute(
                    f"INSERT INTO {table_name} (artf_cd, item_cd, item_nm, disp_ord, out_yn) VALUES (%s, %s, %s, %s, %s)",
                    (layout_code, str(row.ITEM_CD), str(row.ITEM_NM).strip(), int(row.DISP_ORD), bool(row.OUT_YN)),
                )
        connection.commit()


def table_frame(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str]) -> pd.DataFrame:
    query = f'''SELECT T.mpg_id AS "MPG_ID", T.prj_cd AS "PRJ_CD", T.sbj_area_cd AS "SBJ_AREA_CD", S.sbj_area_nm AS "SBJ_AREA_NM",
                      T.src_conn_id AS "SRC_CONN_ID", T.src_sch_nm AS "SRC_SCH_NM", T.src_tbl_nm AS "SRC_TBL_NM",
                      T.tgt_conn_id AS "TGT_CONN_ID", T.tgt_sch_nm AS "TGT_SCH_NM", T.tgt_tbl_nm AS "TGT_TBL_NM", T.tgt_dist_style AS "TGT_DIST_STYLE", T.tgt_dist_key_col AS "TGT_DIST_KEY_COL", T.tgt_sort_style AS "TGT_SORT_STYLE", T.tgt_sort_cols AS "TGT_SORT_COLS", T.tgt_encd_auto_yn AS "TGT_ENCD_AUTO_YN",
                      T.load_sts_cd AS "LOAD_STS_CD", T.incr_basis_cd AS "INCR_BASIS_CD", T.incr_basis_col_nm AS "INCR_BASIS_COL_NM", T.parl_mthd_cd AS "PARL_MTHD_CD", T.parl_cnd_arr AS "PARL_CND_ARR", T.meta_ver_no AS "META_VER_NO"
                  FROM {qualified(schema_name, "tb_mig_tbl_mpg")} T
                  LEFT JOIN {qualified(schema_name, "tb_mig_sbj_area")} S ON S.sbj_area_cd = T.sbj_area_cd
                 WHERE T.active_yn = TRUE
                 ORDER BY T.prj_cd, T.sbj_area_cd, T.mpg_id'''
    return query_frame(values, query)


def column_frame(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str]) -> pd.DataFrame:
    query = f'''SELECT T.mpg_id AS "MPG_ID", T.prj_cd AS "PRJ_CD", T.sbj_area_cd AS "SBJ_AREA_CD", T.src_sch_nm AS "SRC_SCH_NM", T.src_tbl_nm AS "SRC_TBL_NM", T.tgt_sch_nm AS "TGT_SCH_NM", T.tgt_tbl_nm AS "TGT_TBL_NM",
                      C.col_ord AS "COL_ORD", C.src_col_no AS "SRC_COL_NO", C.src_col_nm AS "SRC_COL_NM", C.src_data_type AS "SRC_DATA_TYPE", C.src_null_yn AS "SRC_NULL_YN", C.src_key_role_cd AS "SRC_KEY_ROLE_CD", C.tgt_col_no AS "TGT_COL_NO", C.tgt_col_nm AS "TGT_COL_NM", C.tgt_data_type AS "TGT_DATA_TYPE", C.tgt_null_yn AS "TGT_NULL_YN", C.tgt_key_role_cd AS "TGT_KEY_ROLE_CD", C.trnsf_expr AS "TRNSF_EXPR", C.dflt_expr AS "DFLT_EXPR", C.sum_vald_yn AS "SUM_VALD_YN", C.hsh_vald_yn AS "HSH_VALD_YN"
                  FROM {qualified(schema_name, "tb_mig_col_mpg")} C
                  JOIN {qualified(schema_name, "tb_mig_tbl_mpg")} T ON T.mpg_id = C.mpg_id
                 WHERE T.active_yn = TRUE AND C.active_yn = TRUE
                 ORDER BY T.prj_cd, T.sbj_area_cd, T.mpg_id, C.col_ord'''
    return query_frame(values, query)


def mapping_frame(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str]) -> pd.DataFrame:
    query = f'''SELECT T.mpg_id AS "MPG_ID", T.prj_cd AS "PRJ_CD", T.sbj_area_cd AS "SBJ_AREA_CD", S.sbj_area_nm AS "SBJ_AREA_NM", T.src_conn_id AS "SRC_CONN_ID", T.src_sch_nm AS "SRC_SCH_NM", T.src_tbl_nm AS "SRC_TBL_NM", T.tgt_conn_id AS "TGT_CONN_ID", T.tgt_sch_nm AS "TGT_SCH_NM", T.tgt_tbl_nm AS "TGT_TBL_NM", T.tgt_dist_style AS "TGT_DIST_STYLE", T.tgt_dist_key_col AS "TGT_DIST_KEY_COL", T.tgt_sort_style AS "TGT_SORT_STYLE", T.tgt_sort_cols AS "TGT_SORT_COLS", T.load_sts_cd AS "LOAD_STS_CD", T.incr_basis_cd AS "INCR_BASIS_CD", T.incr_basis_col_nm AS "INCR_BASIS_COL_NM", T.parl_mthd_cd AS "PARL_MTHD_CD", T.parl_cnd_arr AS "PARL_CND_ARR", C.col_ord AS "COL_ORD", C.src_col_no AS "SRC_COL_NO", C.src_col_nm AS "SRC_COL_NM", C.src_data_type AS "SRC_DATA_TYPE", C.src_null_yn AS "SRC_NULL_YN", C.src_key_role_cd AS "SRC_KEY_ROLE_CD", C.tgt_col_no AS "TGT_COL_NO", C.tgt_col_nm AS "TGT_COL_NM", C.tgt_data_type AS "TGT_DATA_TYPE", C.tgt_null_yn AS "TGT_NULL_YN", C.tgt_key_role_cd AS "TGT_KEY_ROLE_CD", C.trnsf_expr AS "TRNSF_EXPR", C.dflt_expr AS "DFLT_EXPR", C.sum_vald_yn AS "SUM_VALD_YN", C.hsh_vald_yn AS "HSH_VALD_YN"
                  FROM {qualified(schema_name, "tb_mig_col_mpg")} C
                  JOIN {qualified(schema_name, "tb_mig_tbl_mpg")} T ON T.mpg_id = C.mpg_id AND T.active_yn = TRUE
                  LEFT JOIN {qualified(schema_name, "tb_mig_sbj_area")} S ON S.sbj_area_cd = T.sbj_area_cd
                 WHERE C.active_yn = TRUE
                 ORDER BY T.prj_cd, T.sbj_area_cd, T.mpg_id, C.col_ord'''
    return query_frame(values, query)


def unit_test_frame(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], start_dt: date, end_dt: date) -> pd.DataFrame:
    query = f'''SELECT L.wrk_dt AS "WRK_DT", L.dag_nm AS "DAG_NM", L.dag_run_id AS "DAG_RUN_ID", L.task_nm AS "TASK_NM", L.mpg_id AS "MPG_ID", L.manf_id AS "MANF_ID", COALESCE(T.src_sch_nm || '.' || T.src_tbl_nm, '') AS "SRC_TBL", COALESCE(T.tgt_sch_nm || '.' || T.tgt_tbl_nm, '') AS "TGT_TBL", L.wrk_step_cd AS "WRK_STEP_CD", L.wrk_sts_cd AS "WRK_STS_CD", L.s3_manf_path AS "S3_MANF_PATH", L.load_mthd_cd AS "LOAD_MTHD_CD", L.ins_scope_cd AS "INS_SCOPE_CD", L.src_row_cnt AS "SRC_ROW_CNT", L.tgt_row_cnt AS "TGT_ROW_CNT", L.src_size_byte AS "SRC_SIZE_BYTE", L.tgt_size_byte AS "TGT_SIZE_BYTE", L.wrk_stt_dtm AS "WRK_STT_DTM", L.wrk_end_dtm AS "WRK_END_DTM", L.wrk_elps_sec AS "WRK_ELPS_SEC", L.wrk_cnd_val AS "WRK_CND_VAL", L.sql_file_path AS "SQL_FILE_PATH", L.log_file_path AS "LOG_FILE_PATH", L.wrk_msg AS "WRK_MSG"
                  FROM {qualified(schema_name, "tb_mig_run_log")} L
                  LEFT JOIN {qualified(schema_name, "tb_mig_tbl_mpg")} T ON T.mpg_id = L.mpg_id
                 WHERE L.mpg_id IS NOT NULL AND L.wrk_dt BETWEEN %s AND %s
                 ORDER BY L.wrk_dt DESC, L.run_hist_id DESC'''
    return query_frame(values, query, (start_dt, end_dt))


def integration_test_frame(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], start_dt: date, end_dt: date) -> pd.DataFrame:
    query = f'''SELECT MIN(wrk_dt) AS "WRK_DT", dag_nm AS "DAG_NM", dag_run_id AS "DAG_RUN_ID", COUNT(DISTINCT mpg_id) AS "TBL_CNT", SUM(CASE WHEN wrk_sts_cd = 'SUCCESS' THEN 1 ELSE 0 END) AS "SUC_CNT", SUM(CASE WHEN wrk_sts_cd = 'FAILED' THEN 1 ELSE 0 END) AS "FAIL_CNT", MIN(wrk_stt_dtm) AS "STT_DTM", MAX(wrk_end_dtm) AS "END_DTM", DATEDIFF(second, MIN(wrk_stt_dtm), MAX(COALESCE(wrk_end_dtm, wrk_stt_dtm))) AS "ELPS_SEC", CASE WHEN SUM(CASE WHEN wrk_sts_cd = 'FAILED' THEN 1 ELSE 0 END) > 0 THEN 'FAILED' WHEN SUM(CASE WHEN wrk_sts_cd = 'SUCCESS' THEN 1 ELSE 0 END) > 0 THEN 'SUCCESS' ELSE 'RUNNING' END AS "WRK_STS_CD"
                  FROM {qualified(schema_name, "tb_mig_run_log")}
                 WHERE wrk_dt BETWEEN %s AND %s
                 GROUP BY dag_nm, dag_run_id
                 ORDER BY MIN(wrk_dt) DESC, dag_nm, dag_run_id'''
    return query_frame(values, query, (start_dt, end_dt))


def validation_frames(query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], start_dt: date, end_dt: date) -> dict[str, pd.DataFrame]:
    query = f'''SELECT exec_run_id AS "EXEC_RUN_ID", dag_nm AS "DAG_NM", dag_run_id AS "DAG_RUN_ID", mpg_id AS "MPG_ID", vald_dvsn_cd AS "VALD_DVSN_CD", s3_manf_path AS "S3_MANF_PATH", cnt_vald_sts_cd AS "CNT_VALD_STS_CD", src_cnt AS "SRC_CNT", tgt_cnt AS "TGT_CNT", cnt_diff AS "CNT_DIFF", sum_vald_sts_cd AS "SUM_VALD_STS_CD", hsh_vald_sts_cd AS "HSH_VALD_STS_CD", vald_sts_cd AS "VALD_STS_CD", vald_stt_dtm AS "VALD_STT_DTM", vald_end_dtm AS "VALD_END_DTM", vald_elps_sec AS "VALD_ELPS_SEC", vald_msg AS "VALD_MSG"
                  FROM {qualified(schema_name, "tb_mig_vald_rslt")}
                 WHERE CAST(vald_stt_dtm AS DATE) BETWEEN %s AND %s
                 ORDER BY vald_hist_id DESC'''
    return {"VALD_RSLT": query_frame(values, query, (start_dt, end_dt))}


def apply_layout(frame: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        columns = items.loc[items.OUT_YN, "ITEM_NM"].tolist()
        return pd.DataFrame(columns=columns)
    source = {str(column).upper(): column for column in frame.columns}
    visible = items.loc[items.OUT_YN].sort_values("DISP_ORD")
    selected = [(str(row.ITEM_CD).upper(), str(row.ITEM_NM)) for row in visible.itertuples(index=False) if str(row.ITEM_CD).upper() in source]
    if not selected:
        raise ValueError("출력할 산출물 항목이 없습니다.")
    output = frame.loc[:, [source[code] for code, _ in selected]].copy()
    output.columns = [name for _, name in selected]
    return output.where(pd.notna(output), None)


def excel_bytes(sheets: list[tuple[str, pd.DataFrame]]) -> bytes:
    from openpyxl.styles import Border, Font, PatternFill, Side

    output = BytesIO()
    border = Border(left=Side(style="thin", color="D7DEE8"), right=Side(style="thin", color="D7DEE8"), top=Side(style="thin", color="D7DEE8"), bottom=Side(style="thin", color="D7DEE8"))
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, frame in sheets:
            frame.to_excel(writer, sheet_name=sheet_name[:31], index=False)
            sheet = writer.sheets[sheet_name[:31]]
            sheet.freeze_panes = "A2"
            sheet.auto_filter.ref = sheet.dimensions
            for row_number, row in enumerate(sheet.iter_rows(), start=1):
                for cell in row:
                    cell.font = Font(name="맑은 고딕", size=10, bold=row_number == 1)
                    cell.border = border
                    if row_number == 1:
                        cell.fill = PatternFill(fill_type="solid", fgColor="FFF2CC")
            for cells in sheet.columns:
                width = max(len(str(cell.value or "")) for cell in cells) + 2
                sheet.column_dimensions[cells[0].column_letter].width = min(max(width, 11), 42)
    return output.getvalue()


def save_artifact(document_code: str, created: datetime, data: bytes) -> Path:
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    path = ARTIFACT_ROOT / f"{document_code}_{created:%Y%m%d%H%M%S}.xlsx"
    path.write_bytes(data)
    return path


def document_frames(document_code: str, query_frame: Callable[..., pd.DataFrame], values: dict[str, Any], schema_name: str, qualified: Callable[[str, str], str], start_dt: date, end_dt: date) -> dict[str, pd.DataFrame]:
    if document_code == "TBL_DFN":
        return {"TBL_DFN": table_frame(query_frame, values, schema_name, qualified)}
    if document_code == "COL_DFN":
        return {"COL_DFN": column_frame(query_frame, values, schema_name, qualified)}
    if document_code == "MPG_DFN":
        return {"MPG_DFN": mapping_frame(query_frame, values, schema_name, qualified)}
    if document_code == "UTEST_RSLT":
        return {"UTEST_RSLT": unit_test_frame(query_frame, values, schema_name, qualified, start_dt, end_dt)}
    if document_code == "ITEST_RSLT":
        return {"ITEST_RSLT": integration_test_frame(query_frame, values, schema_name, qualified, start_dt, end_dt)}
    return validation_frames(query_frame, values, schema_name, qualified, start_dt, end_dt)


def render_artifacts(values: dict[str, Any], schema_name: str, can_edit: bool, query_frame: Callable[..., pd.DataFrame], connect: Callable[[dict[str, Any]], Any], qualified: Callable[[str, str], str]) -> None:
    mode = st.segmented_control("산출물 업무", ["📥 산출물 생성", "🧩 레이아웃 정의"], default="📥 산출물 생성", label_visibility="collapsed")
    if mode == "🧩 레이아웃 정의":
        layout_code = st.selectbox("산출물", list(LAYOUTS), format_func=layout_label)
        try:
            configured = read_layout(query_frame, values, schema_name, qualified, layout_code)
            items = configured_items(configured, layout_code)
        except Exception as error:
            st.error(f"산출물 레이아웃 조회 실패: {error}", icon=":material/error:")
            return
        edited = st.data_editor(items, hide_index=True, disabled=["ITEM_CD"], column_config={"ITEM_CD": "항목코드", "ITEM_NM": "항목명", "DISP_ORD": st.column_config.NumberColumn("출력순서", min_value=1, step=1), "OUT_YN": st.column_config.CheckboxColumn("출력")}, key=f"artf_layout_{layout_code}")
        if st.button("레이아웃 저장", icon=":material/save:", type="primary", disabled=not can_edit):
            try:
                save_layout(connect, values, schema_name, qualified, layout_code, edited)
                st.success("산출물 레이아웃을 저장했습니다.", icon=":material/check_circle:")
            except Exception as error:
                st.error(f"산출물 레이아웃 저장 실패: {error}", icon=":material/error:")
        return

    controls = st.columns([2, 1, 1])
    with controls[0]:
        document_code = st.selectbox("생성 산출물", list(DOCUMENTS), format_func=lambda code: DOCUMENTS[code][0])
    with controls[1]:
        start_dt = st.date_input("작업 시작일", value=date.today() - timedelta(days=7))
    with controls[2]:
        end_dt = st.date_input("작업 종료일", value=date.today())
    if end_dt < start_dt:
        st.error("작업 종료일은 작업 시작일보다 빠를 수 없습니다.", icon=":material/error:")
        return
    if st.button("산출물 생성", icon=":material/description:", type="primary"):
        try:
            frames = document_frames(document_code, query_frame, values, schema_name, qualified, start_dt, end_dt)
            sheets: list[tuple[str, pd.DataFrame]] = []
            for layout_code in DOCUMENTS[document_code][1]:
                items = configured_items(read_layout(query_frame, values, schema_name, qualified, layout_code), layout_code)
                sheets.append((LAYOUTS[layout_code]["sheet"], apply_layout(frames.get(layout_code, pd.DataFrame()), items)))
            created = datetime.now()
            data = excel_bytes(sheets)
            path = save_artifact(document_code, created, data)
            st.session_state["mig_artifact_payload"] = {
                "document_code": document_code,
                "data": data,
                "rows": sum(len(frame) for _, frame in sheets),
                "created": created,
                "path": path.relative_to(Path(__file__).parent.parent).as_posix(),
            }
        except Exception as error:
            st.error(f"산출물 생성 실패: {error}", icon=":material/error:")
    payload = st.session_state.get("mig_artifact_payload")
    if payload and payload["document_code"] == document_code:
        st.success(f"{payload['rows']:,}건의 산출물을 만들었습니다.", icon=":material/check_circle:")
        st.caption(f"보관 위치: {payload['path']}")
        st.download_button("엑셀 다운로드", data=payload["data"], file_name=f"{DOCUMENTS[document_code][0]}_{payload['created']:%Y%m%d%H%M%S}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", icon=":material/download:")

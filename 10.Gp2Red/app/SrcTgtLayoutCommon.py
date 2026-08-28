from __future__ import annotations

from typing import Any

import pandas as pd

from SrcTgtRuntime import qualified, query_frame, text


def dates(values: dict[str, Any], schema_name: str, connection_id: str) -> list[str]:
    frame = query_frame(values, f"SELECT DISTINCT std_dt FROM {qualified(schema_name, 'tb_mig_src_layout')} WHERE src_conn_id = %s ORDER BY std_dt", (connection_id,))
    return [text(value) for value in frame.std_dt.tolist()]


def comparison(values: dict[str, Any], schema_name: str, connection_id: str, before: str, after: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    query = f'''SELECT std_dt, src_sch_nm, src_tbl_nm, src_col_no, src_col_nm, src_data_type, src_data_len, src_pk_yn, src_null_yn
                  FROM {qualified(schema_name, 'tb_mig_src_layout')}
                 WHERE src_conn_id = %s AND std_dt IN (%s, %s)'''
    frame = query_frame(values, query, (connection_id, before, after))
    old = frame.loc[frame.std_dt.map(text).eq(before)].copy()
    new = frame.loc[frame.std_dt.map(text).eq(after)].copy()
    keys = ["src_sch_nm", "src_tbl_nm", "src_col_no"]
    merged = old.merge(new, on=keys, how="outer", suffixes=("_BF", "_AF"), indicator=True)
    fields = ["src_col_nm", "src_data_type", "src_data_len", "src_pk_yn", "src_null_yn"]
    changed = merged.loc[(merged._merge.ne("both")) | (merged.apply(lambda row: any(text(row[f"{field}_BF"]) != text(row[f"{field}_AF"]) for field in fields), axis=1))].copy()
    changed["CHG_DVSN"] = changed._merge.map({"left_only": "삭제", "right_only": "신규", "both": "변경"})
    tables = changed.groupby(["src_sch_nm", "src_tbl_nm", "CHG_DVSN"], dropna=False).size().reset_index(name="COL_CNT")
    return tables, changed

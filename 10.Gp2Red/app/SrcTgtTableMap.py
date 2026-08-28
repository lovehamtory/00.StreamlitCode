from __future__ import annotations

from typing import Any

import pandas as pd

from SrcTgtRuntime import qualified, query_frame


TABLE_COLUMNS = [
    "mpg_id", "prj_cd", "sbj_area_cd", "tgt_conn_id", "tgt_sch_nm", "tgt_tbl_nm", "tgt_tbl_cmt", "src_conn_id", "src_sch_nm", "src_tbl_nm",
    "load_sts_cd", "sys_col_nm_arr", "sys_col_fmt_cd", "incr_mthd_cd", "src_incr_col_nm_arr", "parl_mthd_cd", "parl_cnd_arr", "meta_ver_no",
]


def table_maps(values: dict[str, Any], schema_name: str) -> pd.DataFrame:
    columns = ", ".join(f"T.{column}" for column in TABLE_COLUMNS if column not in {"src_conn_id", "tgt_conn_id"})
    query = f"SELECT {columns}, A.src_conn_id, A.tgt_conn_id FROM {qualified(schema_name, 'tb_mig_tbl_mpg')} T JOIN {qualified(schema_name, 'tb_mig_sbj_area')} A ON A.sbj_area_cd = T.sbj_area_cd WHERE T.active_yn = TRUE ORDER BY T.prj_cd, T.sbj_area_cd, T.mpg_id"
    return query_frame(values, query)

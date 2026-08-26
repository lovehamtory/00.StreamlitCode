from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import sqlglot


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "app"))
sys.path.insert(0, str(PROJECT_ROOT / "dag"))

import SrcTgtDagGenerator as dag_generator
import SrcTgtConnection as connection
import SrcTgtDataType as data_type
import SrcTgtLayoutHistory as layout_history
import SrcTgtLoadState as load_state
import SrcTgtMapping as mapping
import SrcTgtSetup as setup
import SrcTgtTargetReflection as target_reflection
import SrcTgtRuntime as runtime
from common.mig_step_runtime import execute_logged_step


class VirtualWorkflowTest(unittest.TestCase):
    def test_initial_metadata_contract(self) -> None:
        ddl = (PROJECT_ROOT / "sql" / "01_mig_metadata_ddl.sql").read_text(encoding="utf-8").upper()
        self.assertIn("TB_MIG_SBJ_AREA", ddl)
        self.assertIn("TB_MIG_CONN", ddl)
        self.assertIn("CHAR_LEN_MUL", ddl)
        self.assertIn("TB_MIG_TBL_MPG", ddl)
        self.assertIn("TB_MIG_VALD_RSLT", ddl)
        self.assertIn("TB_MIG_VALD_COL_RSLT", ddl)
        self.assertIn("TB_MIG_TBL_DEP", ddl)
        self.assertIn("TB_MIG_SBJ_DEP", ddl)
        self.assertIn("S3_STG_PATH", ddl)
        self.assertIn("SCHD_CD", ddl)
        self.assertIn("'FULL_CTL'", ddl)
        self.assertIn("'INCR_CTL'", ddl)
        self.assertIn("TB_MIG_S3_MANF", ddl)
        self.assertIn("TB_MIG_TBL_LOAD_HIST", ddl)
        self.assertIn("PARL_MTHD_CD", ddl)
        self.assertIn("PARL_CND_ARR", ddl)
        self.assertNotIn("PARL_BASIS_COL_NM", ddl)
        self.assertNotIn("INCR_WHERE_TMPL", ddl)
        self.assertNotIn("TB_MIG_ONCE_WRK", ddl)
        self.assertNotIn("TB_MIG_ONCE_TBL", ddl)
        self.assertNotIn("TB_MIG_TBL_RLOD_REQ", ddl)
        self.assertNotIn("TB_MIG_USR", ddl)
        self.assertNotIn("S3_CONN_ID", ddl)
        self.assertNotIn("RUN_WAVE_NO", ddl)
        self.assertNotIn("INCR_SCHD_CD", ddl)
        self.assertNotIn("DROP SCHEMA", ddl)
        for table in ("TB_MIG_ARTF_ITEM", "TB_MIG_VALD_COL_RSLT", "TB_MIG_VALD_RSLT", "TB_MIG_TBL_LOAD_HIST", "TB_MIG_S3_MANF", "TB_MIG_RUN_LOG", "TB_MIG_TBL_DEP", "TB_MIG_CONN", "TB_MIG_COL_MPG", "TB_MIG_TBL_MPG", "TB_MIG_SBJ_DAG_MPG", "TB_MIG_SBJ_DEP", "TB_MIG_SBJ_AREA"):
            self.assertIn(f"DROP TABLE IF EXISTS MIG_META.{table}", ddl)

    def test_connection_master_contract(self) -> None:
        self.assertFalse((PROJECT_ROOT / "sql" / "02_mig_connection_migration.sql").exists())
        self.assertEqual(connection.connection_id("src_ora_01"), "SRC_ORA_01")
        with self.assertRaises(ValueError):
            connection.connection_id("01_SRC")
        frame = pd.DataFrame([
            {"conn_id": "SRC_GP", "conn_nm": "Greenplum", "dbms_cd": "GREENPLUM", "char_len_mul": 3, "s3_stg_path": None, "sec_sect_nm": "greenplum", "active_yn": True},
            {"conn_id": "TGT_RED", "conn_nm": "Redshift", "dbms_cd": "REDSHIFT", "char_len_mul": 3, "s3_stg_path": "s3://bucket/migration", "sec_sect_nm": "redshift_sql", "active_yn": True},
        ])
        self.assertEqual(connection.connection_ids(frame), ["SRC_GP", "TGT_RED"])
        self.assertEqual(connection.connection_label(frame, "TGT_RED"), "TGT_RED · Redshift · REDSHIFT")

    def test_redshift_metadata_ddl_static_parse(self) -> None:
        source = (PROJECT_ROOT / "sql" / "01_mig_metadata_ddl.sql").read_text(encoding="utf-8")
        expressions = sqlglot.parse(source, read="redshift")
        self.assertGreater(len(expressions), 10)

    def test_initial_setup_schema_contract(self) -> None:
        self.assertEqual(setup.schema_name("migration_meta"), "migration_meta")
        with self.assertRaises(ValueError):
            setup.schema_name("migration-meta")
        ddl = setup.ddl_source("migration_meta")
        self.assertNotIn("CREATE SCHEMA", ddl.upper())
        self.assertNotIn("MIG_META", ddl)
        self.assertNotIn("DROP SCHEMA", ddl.upper())
        self.assertIn("DROP TABLE", ddl.upper())
        names = setup.backup_table_names("migration_meta", "20260825")
        self.assertIn(("tb_mig_tbl_mpg", "tb_mig_tbl_mpg_20260825"), names)

    def test_source_layout_to_target_mapping_contract(self) -> None:
        source = pd.DataFrame([
            {"COL_ORD": 1, "SRC_COL_NO": 1, "SRC_COL_NM": "CUST_ID", "SRC_DATA_TYPE": "BIGINT", "SRC_NULL_YN": False, "SRC_KEY_ROLE_CD": "PK"},
            {"COL_ORD": 2, "SRC_COL_NO": 2, "SRC_COL_NM": "CUST_NM", "SRC_DATA_TYPE": "VARCHAR", "SRC_NULL_YN": True, "SRC_KEY_ROLE_CD": None},
        ])
        with patch.object(mapping, "source_layout_table", return_value=("public", "TB_TABLE_LAYOUT_GP")):
            columns = mapping.source_columns(lambda values, query, parameters: source, {}, lambda schema_name, table_name: f'"{schema_name}"."{table_name}"', "SRC_GP", "20260825", "SRC", "CUSTOMER")
        self.assertEqual(columns.loc[0, "TGT_COL_NM"], "CUST_ID")
        self.assertEqual(columns.loc[1, "TGT_DATA_TYPE"], "VARCHAR(65535)")
        row = mapping.defaults({"PRJ_CD": "PRJ1", "SBJ_AREA_CD": "CORE", "SRC_SCH_NM": "SRC", "SRC_TBL_NM": "CUSTOMER", "TGT_SCH_NM": "DWH", "TGT_TBL_NM": "CUSTOMER"})
        self.assertEqual(row["LOAD_STS_CD"], "FULL")
        self.assertEqual(row["TGT_DIST_STYLE"], "AUTO")

    def test_existing_target_automatic_mapping(self) -> None:
        source = pd.DataFrame([
            {"SRC_COL_NM": "CUST_ID", "TGT_COL_NO": 1, "TGT_COL_NM": "CUST_ID", "TGT_DATA_TYPE": "BIGINT", "TGT_NULL_YN": False, "TGT_KEY_ROLE_CD": "PK"},
            {"SRC_COL_NM": "CUST_NM", "TGT_COL_NO": 2, "TGT_COL_NM": "CUST_NM", "TGT_DATA_TYPE": "VARCHAR", "TGT_NULL_YN": True, "TGT_KEY_ROLE_CD": None},
        ])
        target = pd.DataFrame([
            {"TGT_COL_NO": 1, "TGT_COL_NM": "CUST_ID", "TGT_DATA_TYPE": "BIGINT", "TGT_NULL_YN": False, "TGT_KEY_ROLE_CD": "PK"},
            {"TGT_COL_NO": 2, "TGT_COL_NM": "CUST_NM", "TGT_DATA_TYPE": "VARCHAR", "TGT_NULL_YN": True, "TGT_KEY_ROLE_CD": None},
        ])
        converted, matched = mapping.automatic_columns(source, target)
        self.assertEqual(matched, 2)
        self.assertEqual(converted.loc[1, "TGT_COL_NM"], "CUST_NM")

    def test_selected_date_and_schema_layout_replacement(self) -> None:
        calls: list[tuple[str, tuple[object, ...]]] = []

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, query, parameters):
                calls.append((query, parameters))

            def executemany(self, query, parameters):
                calls.append((query, tuple(parameters)))

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def cursor(self):
                return Cursor()

            def commit(self):
                return None

        layout = pd.DataFrame([["SRC_GP", "20260825", "SRC_A", "CUSTOMER", "고객", 1, "CUST_ID", "", "bigint", "", "Y", "NO"]], columns=layout_history.LAYOUT_COLUMNS)
        with patch.object(layout_history, "connect", lambda target: Connection()):
            saved = layout_history.save_layout({}, "meta", "TB_TABLE_LAYOUT_GP", "SRC_GP", "20260825", ["SRC_A", "SRC_B"], layout)
        self.assertEqual(saved, 1)
        self.assertIn("COALESCE(SRC_CONN_ID, 'SRC_GP')=%s AND STD_DT=%s AND OWNER IN (%s, %s)", calls[0][0])
        self.assertEqual(calls[0][1], ("SRC_GP", "20260825", "SRC_A", "SRC_B"))

    def test_layout_change_detection(self) -> None:
        before = pd.DataFrame([["SRC_GP", "20260824", "SRC", "CUSTOMER", "고객", 1, "CUST_ID", "", "bigint", "", "Y", "NO"]], columns=layout_history.LAYOUT_COLUMNS)
        after = pd.DataFrame([["SRC_GP", "20260825", "SRC", "CUSTOMER", "고객", 1, "CUST_ID", "", "bigint", "", "Y", "NO"], ["SRC_GP", "20260825", "SRC", "CUSTOMER", "고객", 2, "CUST_NM", "", "character varying", "100", "", "YES"]], columns=layout_history.LAYOUT_COLUMNS)
        tables, columns = layout_history.compare_layouts(before, after)
        self.assertEqual(tables.iloc[0]["구분"], "변경")
        self.assertEqual(columns.iloc[0]["구분"], "신규")

    def test_target_reflection_ddl_contract(self) -> None:
        table = pd.Series({
            "tgt_sch_nm": "DWH", "tgt_tbl_nm": "CUSTOMER", "tgt_dist_style": "KEY", "tgt_dist_key_col": "CUST_ID",
            "tgt_sort_style": "COMPOUND", "tgt_sort_cols": "CUST_ID, LOAD_DT", "tgt_encd_auto_yn": True,
        })
        columns = pd.DataFrame([
            {"col_ord": 1, "tgt_col_nm": "CUST_ID", "tgt_data_type": "BIGINT", "tgt_null_yn": False, "dflt_expr": None},
            {"col_ord": 2, "tgt_col_nm": "LOAD_DT", "tgt_data_type": "DATE", "tgt_null_yn": True, "dflt_expr": "CURRENT_DATE"},
        ])
        ddl = target_reflection.ddl_for(table, columns)
        self.assertIn('CREATE TABLE IF NOT EXISTS "DWH"."CUSTOMER"', ddl)
        self.assertIn('DISTKEY ("CUST_ID")', ddl)
        self.assertIn('COMPOUND SORTKEY ("CUST_ID", "LOAD_DT")', ddl)
        self.assertIn('"LOAD_DT" TIMESTAMP DEFAULT CURRENT_DATE', ddl)
        displayed = target_reflection.target_columns(columns)
        self.assertIn("대상 컬럼명", displayed.columns)

    def test_redshift_standard_type_contract(self) -> None:
        self.assertEqual(data_type.redshift_type("bpchar", "12"), "VARCHAR(12)")
        self.assertEqual(data_type.redshift_type("bpchar", "12", 3), "VARCHAR(36)")
        self.assertEqual(data_type.redshift_type("nvarchar(40)", None, 2), "VARCHAR(80)")
        self.assertEqual(data_type.redshift_type("character varying(50)"), "VARCHAR(50)")
        self.assertEqual(data_type.redshift_type("numeric(18,2)"), "DECIMAL(18,2)")
        self.assertEqual(data_type.redshift_type("bigint"), "DECIMAL(38,0)")
        self.assertEqual(data_type.redshift_type("double precision"), "DECIMAL(38,10)")
        self.assertEqual(data_type.redshift_type("boolean"), "BOOLEAN")
        self.assertEqual(data_type.redshift_type("bit"), "BOOLEAN")
        self.assertEqual(data_type.redshift_type("date"), "TIMESTAMP")
        self.assertEqual(data_type.redshift_type("datetime2"), "TIMESTAMP")
        with self.assertRaises(ValueError):
            data_type.redshift_type("interval")

    def test_target_reflection_source_date_contract(self) -> None:
        calls: list[tuple[str, tuple[object, ...]]] = []

        def query(values, query, parameters=()):
            calls.append((query, parameters))
            return pd.DataFrame(columns=["원천 컬럼순번", "원천 컬럼명"])

        with patch.object(target_reflection, "query_frame", query):
            target_reflection.source_layout({}, "meta", "TB_TABLE_LAYOUT_GP", "SRC_GP", "20260825", "SRC", "CUSTOMER")
        self.assertIn("COALESCE(src_conn_id, 'SRC_GP') = %s", calls[0][0])
        self.assertIn("std_dt = %s", calls[0][0])
        self.assertIn("UPPER(owner) = UPPER(%s)", calls[0][0])
        self.assertEqual(calls[0][1], ("SRC_GP", "20260825", "SRC", "CUSTOMER"))

    def test_target_reflection_design_save_contract(self) -> None:
        calls: list[tuple[str, tuple[object, ...]]] = []

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, query, parameters):
                calls.append((query, parameters))

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def cursor(self):
                return Cursor()

            def commit(self):
                return None

        context = runtime.RuntimeContext({}, "mig_meta")
        with patch.object(target_reflection, "connect", lambda values: Connection()):
            target_reflection.save_target_design(context, 10, {"tgt_dist_style": "KEY", "tgt_dist_key_col": "CUST_ID", "tgt_sort_style": "AUTO", "tgt_sort_cols": "", "tgt_encd_auto_yn": True})
        self.assertIn("tgt_ddl_sql = NULL", calls[0][0])
        self.assertEqual(calls[0][1][-1], 10)

    def test_structure_navigation_contract(self) -> None:
        control = (PROJECT_ROOT / "app" / "SrcTgtControl.py").read_text(encoding="utf-8")
        structure = (PROJECT_ROOT / "app" / "SrcTgtLayoutHistory.py").read_text(encoding="utf-8")
        orchestrator = (PROJECT_ROOT / "app" / "SrcTgtOrchestrator.py").read_text(encoding="utf-8")
        self.assertNotIn('"🧱 물리설계"', control)
        self.assertNotIn('"🧾 DDL"', control)
        self.assertIn('"원천 레이아웃", "대상 반영안"', structure)
        self.assertIn('title="구조조회"', orchestrator)
        self.assertIn('title="일회성 이관 실행"', orchestrator)
        self.assertIn('title="스냅샷 복구"', orchestrator)
        self.assertTrue((PROJECT_ROOT / "app" / "SrcTgtReload.py").exists())
        self.assertTrue((PROJECT_ROOT / "app" / "SrcTgtSnapshotRestore.py").exists())
        self.assertFalse((PROJECT_ROOT.parent / "RedshiftSnapshotTableRestore.py").exists())
        snapshot_restore = (PROJECT_ROOT / "app" / "SrcTgtSnapshotRestore.py").read_text(encoding="utf-8")
        self.assertIn("connection_frame", snapshot_restore)
        self.assertIn('eq("REDSHIFT")', snapshot_restore)
        self.assertIn("--restore-worker", snapshot_restore)

    def test_dag_generation_contract(self) -> None:
        s3_source = dag_generator.dag_source("A010001", "A01", "mig_a010001_s3", 1, 2)
        ins_source = dag_generator.dag_source("A010001", "A01", "mig_a010001_ins", 1, 2)
        full_source = dag_generator.dag_source("A010001", "A01", "mig_a010001_full_ctl", 1, 1)
        incr_source = dag_generator.controller_source("A010001", "A01", "INCR_CTL", "DLY_0200")
        orch_source = dag_generator.project_source("PRJ1", [{"sbj_area_cd": "A010001", "pre_sbj_area_cds": ""}, {"sbj_area_cd": "A010002", "pre_sbj_area_cds": "A010001"}], "FULL")
        incr_orch_source = dag_generator.project_source("PRJ1", [{"sbj_area_cd": "A010001", "pre_sbj_area_cds": ""}], "INCR")
        once_tables = [{"mpg_id": 101, "parl_mthd_cd": "WHERE", "parl_cnd_arr": ["abc_dt between '19000101' and '20001231'", "abc_dt between '20010101' and '20261231'"]}]
        once_source = dag_generator.once_controller_source("ONCE_20260826133000_AB12CD", "S3_INS", once_tables, "원천 변경 보정")
        for source, name in ((s3_source, "s3.py"), (ins_source, "ins.py"), (full_source, "full.py"), (incr_source, "incr.py"), (orch_source, "orch.py"), (incr_orch_source, "incr_orch.py"), (once_source, "once.py")):
            compile(source, name, "exec")
        self.assertIn("run_s3", full_source)
        self.assertIn("run_ins", full_source)
        self.assertIn("VALIDATE_SRC_S3", full_source)
        self.assertIn("VALIDATE_S3_TGT", full_source)
        self.assertIn("schedule='0 2 * * *'", incr_source)
        self.assertIn('LOAD_GROUP_CD", "INCR"', incr_source)
        self.assertNotIn("RLOD_REQ", s3_source)
        self.assertIn("column_mappings", s3_source)
        self.assertIn("trnsf_expr", s3_source)
        self.assertIn("def s3_parallel_rows", s3_source)
        self.assertIn("src_where_cnd", s3_source)
        self.assertIn("expand_s3(load_mappings())", s3_source)
        self.assertNotIn("expand_s3(load_mappings())", ins_source)
        self.assertIn("one_time_mapping_rows", once_source)
        self.assertIn("ONE_TIME_REASON", once_source)
        self.assertNotIn("tb_mig_once", once_source)
        self.assertIn("S3_ONLY", dag_generator.once_controller_source("ONCE_20260826133000_AB12CD", "S3_ONLY", once_tables, "원천 변경 보정"))
        self.assertIn("VALIDATE_S3_TGT", once_source)
        self.assertIn("max_active_tis_per_dag=1", once_source)
        self.assertIn("TriggerRule.ALL_DONE", orch_source)
        self.assertIn("run_a010001 >> run_a010002", orch_source)
        self.assertIn("mig_prj1_incr_orch", incr_orch_source)
        self.assertIn("mig_a010001_s3", s3_source)
        self.assertIn("mig_a010001_ins", ins_source)
        self.assertNotIn("MERGE", ins_source)

    def test_load_transition_and_parallel_contract(self) -> None:
        plan = load_state.transition_plan("FULL", "INCR", 101, False, "WM_DTM", "UPD_DTM")
        self.assertEqual(plan["runtime_method"], "INCR")
        self.assertEqual(load_state.transition_plan("FULL", "INCR", None, False, "", "")["after"], "INCR")
        with self.assertRaises(ValueError):
            load_state.transition_plan("FULL", "INCR", 101, True, "WM_DTM", "UPD_DTM")
        self.assertEqual(load_state.transition_plan("INCR", "FULL", None, False, "WM_DTM", "UPD_DTM")["after"], "FULL")
        with self.assertRaises(ValueError):
            load_state.transition_plan("REBASE", "INCR", 102, False, "WM_DTM", "UPD_DTM")
        with self.assertRaises(ValueError):
            load_state.transition_plan("FULL", "FULL", 101, False, "WM_DTM", "UPD_DTM")
        self.assertEqual(load_state.normalize_parallel("WHERE", '["abc_dt BETWEEN \'19000101\' AND \'20001231\'", "abc_dt BETWEEN \'20010101\' AND \'20101231\'"]')["count"], 2)
        with self.assertRaises(ValueError):
            load_state.normalize_parallel("WHERE", "[]")
        with self.assertRaises(ValueError):
            load_state.normalize_parallel("WHERE", '["abc_dt = \'20260826\'; DELETE FROM X"]')
        window = load_state.recovery_window("2026-08-25 00:00:00", "2026-08-26 00:00:00", "2026-08-26 01:00:00")
        self.assertEqual(window["basis_start_value"], "2026-08-25 00:00:00")
        with self.assertRaises(ValueError):
            load_state.recovery_window("2026-08-26", "2026-08-26", "2026-08-26")

    def test_virtual_extract_and_load(self) -> None:
        module_name = "virtual_migration_executor"
        module = types.ModuleType(module_name)
        module.run_s3 = lambda record: {"src_row_cnt": 3, "tgt_row_cnt": 3, "s3_manf_path": "s3://bucket/manifest.json"}
        module.run_ins = lambda record: {"src_row_cnt": 3, "tgt_row_cnt": 3}
        sys.modules[module_name] = module
        logs: list[tuple[str, str]] = []
        writer = lambda record, step, status, message: logs.append((step, status))
        record = execute_logged_step({"mpg_id": 1}, "S3", "원천 S3 적재", module_name, writer)
        record = execute_logged_step(record, "INS", "대상 적재", module_name, writer)
        self.assertEqual(record["src_row_cnt"], record["tgt_row_cnt"])
        self.assertEqual(logs, [("S3", "RUNNING"), ("S3", "SUCCESS"), ("INS", "RUNNING"), ("INS", "SUCCESS")])

    def test_virtual_s3_failure_stops_insert(self) -> None:
        module_name = "virtual_failed_migration_executor"
        module = types.ModuleType(module_name)
        calls: list[str] = []

        def fail_s3(record):
            calls.append("S3")
            raise RuntimeError("원천 추출 실패")

        module.run_s3 = fail_s3
        module.run_ins = lambda record: calls.append("INS")
        sys.modules[module_name] = module
        logs: list[tuple[str, str]] = []
        writer = lambda record, step, status, message: logs.append((step, status))
        with self.assertRaisesRegex(RuntimeError, "원천 추출 실패"):
            execute_logged_step({"mpg_id": 1}, "S3", "원천 S3 적재", module_name, writer)
        self.assertEqual(calls, ["S3"])
        self.assertEqual(logs, [("S3", "RUNNING"), ("S3", "FAILED")])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "app"))
sys.path.insert(0, str(PROJECT_ROOT / "dag"))

import SrcTgtDagGenerator as dag_generator
import SrcTgtConnection as connection
import SrcTgtLayoutHistory as layout_history
import SrcTgtMapping as mapping
import SrcTgtSecurity as security
import SrcTgtSetup as setup
import SrcTgtTargetReflection as target_reflection
import SrcTgtUser as user
from common.mig_step_runtime import execute_logged_step


class VirtualWorkflowTest(unittest.TestCase):
    def test_initial_metadata_contract(self) -> None:
        ddl = (PROJECT_ROOT / "sql" / "01_mig_metadata_ddl.sql").read_text(encoding="utf-8").upper()
        self.assertIn("TB_MIG_SBJ_AREA", ddl)
        self.assertIn("TB_MIG_USR_AUTH", ddl)
        self.assertIn("TB_MIG_CONN", ddl)
        self.assertIn("TB_MIG_TBL_MPG", ddl)
        self.assertIn("SCRYPT$32768", ddl)
        self.assertIn("LOWER(S.SBJ_AREA_CD) AS SQL_DIR_NM", ddl)
        self.assertNotIn("DROP SCHEMA", ddl)
        for table in ("TB_MIG_ARTF_ITEM", "TB_MIG_RUN_LOG", "TB_MIG_CONN", "TB_MIG_COL_MPG", "TB_MIG_TBL_MPG", "TB_MIG_USR_AUTH", "TB_MIG_USR", "TB_MIG_SBJ_DAG_MPG", "TB_MIG_SBJ_AREA"):
            self.assertIn(f"DROP TABLE IF EXISTS MIG_META.{table}", ddl)

    def test_connection_master_contract(self) -> None:
        migration_ddl = (PROJECT_ROOT / "sql" / "02_mig_connection_migration.sql").read_text(encoding="utf-8").upper()
        self.assertIn("CREATE TABLE IF NOT EXISTS MIG_META.TB_MIG_CONN", migration_ddl)
        self.assertNotIn("DROP TABLE", migration_ddl)
        self.assertEqual(connection.connection_id("src_ora_01"), "SRC_ORA_01")
        with self.assertRaises(ValueError):
            connection.connection_id("01_SRC")
        frame = pd.DataFrame([
            {"conn_id": "SRC_GP", "conn_nm": "Greenplum", "conn_dvsn_cd": "SRC", "dbms_cd": "GREENPLUM", "sec_sect_nm": "greenplum", "af_conn_id": "SRC_GP", "active_yn": True},
            {"conn_id": "TGT_RED", "conn_nm": "Redshift", "conn_dvsn_cd": "TGT", "dbms_cd": "REDSHIFT", "sec_sect_nm": "redshift_sql", "af_conn_id": "TGT_RED", "active_yn": True},
        ])
        self.assertEqual(connection.connection_ids(frame, "SRC"), ["SRC_GP"])
        self.assertEqual(connection.connection_label(frame, "TGT_RED"), "TGT_RED · Redshift · REDSHIFT")

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

    def test_initial_password_change_contract(self) -> None:
        stored = security.password_hash("temporary-password", salt=b"0123456789abcdef")
        self.assertTrue(security.password_matches("temporary-password", stored))
        self.assertFalse(security.password_matches("different-password", stored))
        security.validate_new_password("1234567890", "1234567890")
        with self.assertRaises(ValueError):
            security.validate_new_password("short", "short")

    def test_source_layout_to_target_mapping_contract(self) -> None:
        source = pd.DataFrame([
            {"COL_ORD": 1, "SRC_COL_NO": 1, "SRC_COL_NM": "CUST_ID", "SRC_DATA_TYPE": "BIGINT", "SRC_NULL_YN": False, "SRC_KEY_ROLE_CD": "PK"},
            {"COL_ORD": 2, "SRC_COL_NO": 2, "SRC_COL_NM": "CUST_NM", "SRC_DATA_TYPE": "VARCHAR", "SRC_NULL_YN": True, "SRC_KEY_ROLE_CD": None},
        ])
        with patch.object(mapping, "source_layout_table", return_value=("public", "TB_TABLE_LAYOUT_GP")):
            columns = mapping.source_columns(lambda values, query, parameters: source, {}, lambda schema_name, table_name: f'"{schema_name}"."{table_name}"', "SRC_GP", "20260825", "SRC", "CUSTOMER")
        self.assertEqual(columns.loc[0, "TGT_COL_NM"], "CUST_ID")
        self.assertEqual(columns.loc[1, "TGT_DATA_TYPE"], "VARCHAR")
        row = mapping.defaults({"PRJ_CD": "PRJ1", "SBJ_AREA_CD": "CORE", "SRC_SCH_NM": "SRC", "SRC_TBL_NM": "CUSTOMER", "TGT_SCH_NM": "DWH", "TGT_TBL_NM": "CUSTOMER"})
        self.assertEqual(row["LOAD_MTHD_CD"], "FULL")
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

    def test_user_group_creation_contract(self) -> None:
        queries: list[tuple[str, tuple[object, ...]]] = []

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, query, parameters):
                queries.append((query, parameters))

            def fetchall(self):
                return []

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def cursor(self):
                return Cursor()

            def commit(self):
                return None

        user.save_new_user(lambda values: Connection(), {}, "mig_meta", lambda schema_name, table_name: f'"{schema_name}"."{table_name}"', "operator1", "운영자", "팀원", "PRJ1", "CORE", "admin")
        roles = [parameters[1] for query, parameters in queries if "INSERT INTO" in query and "tb_mig_usr_auth" in query]
        self.assertEqual(roles, ["READ", "EDIT", "APRV", "EXEC"])

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
        self.assertIn('"LOAD_DT" DATE DEFAULT CURRENT_DATE', ddl)
        displayed = target_reflection.target_columns(columns)
        self.assertIn("대상 컬럼명", displayed.columns)

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

        context = security.AccessContext("admin", {}, "mig_meta", pd.DataFrame(columns=["auth_role_cd", "prj_cd", "sbj_area_cd"]))
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

    def test_dag_generation_contract(self) -> None:
        source = dag_generator.dag_source("CORE", "PRJ", "mig_core", 1, 2)
        compile(source, "mig_core.py", "exec")
        self.assertIn("load_mthd_cd", source)
        self.assertIn("src_af_conn_id", source)
        self.assertNotIn("pre_mpg_id_arr", source)

    def test_virtual_extract_and_load(self) -> None:
        module_name = "virtual_migration_executor"
        module = types.ModuleType(module_name)
        module.run_extract = lambda record: {"src_row_cnt": 3, "src_size_byte": 30}
        module.run_load = lambda record: {"tgt_row_cnt": 3, "tgt_size_byte": 30}
        sys.modules[module_name] = module
        logs: list[tuple[str, str]] = []
        writer = lambda record, step, status, message: logs.append((step, status))
        record = execute_logged_step({"mpg_id": 1}, "EXTRACT", "원천 추출", module_name, writer)
        record = execute_logged_step(record, "LOAD", "대상 적재", module_name, writer)
        self.assertEqual(record["src_row_cnt"], record["tgt_row_cnt"])
        self.assertEqual(logs, [("EXTRACT", "RUNNING"), ("EXTRACT", "SUCCESS"), ("LOAD", "RUNNING"), ("LOAD", "SUCCESS")])


if __name__ == "__main__":
    unittest.main()

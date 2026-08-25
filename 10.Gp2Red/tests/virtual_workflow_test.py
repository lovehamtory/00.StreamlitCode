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
import SrcTgtLayoutHistory as layout_history
import SrcTgtMapping as mapping
import SrcTgtSecurity as security
import SrcTgtSetup as setup
import SrcTgtUser as user
from common.mig_step_runtime import execute_logged_step


class VirtualWorkflowTest(unittest.TestCase):
    def test_initial_metadata_contract(self) -> None:
        ddl = (PROJECT_ROOT / "sql" / "01_mig_metadata_ddl.sql").read_text(encoding="utf-8").upper()
        self.assertIn("TB_MIG_SBJ_AREA", ddl)
        self.assertIn("TB_MIG_USR_AUTH", ddl)
        self.assertIn("TB_MIG_TBL_MPG", ddl)
        self.assertIn("SCRYPT$32768", ddl)
        self.assertIn("LOWER(S.SBJ_AREA_CD) AS SQL_DIR_NM", ddl)
        self.assertNotIn("DROP SCHEMA", ddl)
        for table in ("TB_MIG_ARTF_ITEM", "TB_MIG_RUN_LOG", "TB_MIG_COL_MPG", "TB_MIG_TBL_MPG", "TB_MIG_USR_AUTH", "TB_MIG_USR", "TB_MIG_SBJ_DAG_MPG", "TB_MIG_SBJ_AREA"):
            self.assertIn(f"DROP TABLE IF EXISTS MIG_META.{table}", ddl)

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
            columns = mapping.source_columns(lambda values, query, parameters: source, {}, lambda schema_name, table_name: f'"{schema_name}"."{table_name}"', "20260825", "SRC", "CUSTOMER")
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

        layout = pd.DataFrame([["20260825", "SRC_A", "CUSTOMER", "고객", 1, "CUST_ID", "", "bigint", "", "Y", "NO"]], columns=layout_history.LAYOUT_COLUMNS)
        with patch.object(layout_history, "connect", lambda target: Connection()):
            saved = layout_history.save_layout({}, "meta", "TB_TABLE_LAYOUT_GP", "20260825", ["SRC_A", "SRC_B"], layout)
        self.assertEqual(saved, 1)
        self.assertIn("STD_DT=%s AND OWNER IN (%s, %s)", calls[0][0])
        self.assertEqual(calls[0][1], ("20260825", "SRC_A", "SRC_B"))

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
        before = pd.DataFrame([["20260824", "SRC", "CUSTOMER", "고객", 1, "CUST_ID", "", "bigint", "", "Y", "NO"]], columns=layout_history.LAYOUT_COLUMNS)
        after = pd.DataFrame([["20260825", "SRC", "CUSTOMER", "고객", 1, "CUST_ID", "", "bigint", "", "Y", "NO"], ["20260825", "SRC", "CUSTOMER", "고객", 2, "CUST_NM", "", "character varying", "100", "", "YES"]], columns=layout_history.LAYOUT_COLUMNS)
        tables, columns = layout_history.compare_layouts(before, after)
        self.assertEqual(tables.iloc[0]["구분"], "변경")
        self.assertEqual(columns.iloc[0]["구분"], "신규")
        ddl = layout_history.reference_ddl("SRC", "CUSTOMER", before, after)
        self.assertIn('DROP TABLE IF EXISTS "SRC"."CUSTOMER";', ddl)
        self.assertIn('CREATE TABLE "SRC"."CUSTOMER"', ddl)
        self.assertIn('PRIMARY KEY ("CUST_ID")', ddl)

    def test_dag_generation_contract(self) -> None:
        source = dag_generator.dag_source("CORE", "PRJ", "mig_core", 1, 2)
        compile(source, "mig_core.py", "exec")
        self.assertIn("load_mthd_cd", source)
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

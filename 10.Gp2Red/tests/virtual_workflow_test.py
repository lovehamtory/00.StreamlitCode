from __future__ import annotations

import sys
import types
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import sqlglot
from openpyxl import load_workbook


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "app"))
sys.path.insert(0, str(PROJECT_ROOT / "dag"))

import SrcTgtArtifact as artifact
import SrcTgtDagGenerator as dag_generator
import SrcTgtDataType as data_type
import SrcTgtLoadState as load_state
import SrcTgtMapping as mapping
import SrcTgtSetup as setup
import SrcTgtTargetReflection as target_reflection
from common.mig_step_runtime import execute_logged_step


class VirtualWorkflowTest(unittest.TestCase):
    def test_metadata_contract(self) -> None:
        ddl = (PROJECT_ROOT / "sql" / "01_mig_metadata_ddl.sql").read_text(encoding="utf-8").upper()
        for name in ("TB_MIG_CONN", "TB_MIG_SBJ_AREA", "TB_MIG_SBJ_DAG_MPG", "TB_MIG_SRC_LAYOUT", "TB_MIG_TBL_MPG", "TB_MIG_COL_MPG", "TB_MIG_MPG_CHG_HIST", "TB_MIG_S3_MANF", "TB_MIG_DAG_RUN", "TB_MIG_RUN_LOG", "TB_MIG_VALD_RSLT", "TB_MIG_VALD_COL_RSLT", "TB_MIG_TBL_LOAD_HIST", "TB_MIG_ARTF_ITEM"):
            self.assertIn(name, ddl)
        self.assertNotIn("CREATE TABLE MIG_META.TB_MIG_SBJ_DEP", ddl)
        self.assertNotIn("CREATE TABLE MIG_META.TB_MIG_TBL_DEP", ddl)
        self.assertNotIn("CREATE TABLE MIG_META.TB_MIG_ONCE_WRK", ddl)
        self.assertNotIn("TB_MIG_USR", ddl)
        self.assertNotIn("DROP SCHEMA", ddl)
        self.assertNotIn("TGT_DIST_STYLE", ddl)
        self.assertNotIn("TGT_SORT_STYLE", ddl)
        self.assertIn("SYS_COL_NM_ARR", ddl)
        self.assertIn("SRC_INCR_COL_NM_ARR", ddl)
        self.assertNotIn("BASIS_STT_VAL", ddl)

    def test_redshift_ddl_static_parse(self) -> None:
        source = (PROJECT_ROOT / "sql" / "01_mig_metadata_ddl.sql").read_text(encoding="utf-8")
        expressions = sqlglot.parse(source, read="redshift")
        self.assertGreater(len(expressions), 20)

    def test_initial_setup_contract(self) -> None:
        self.assertIn("tb_mig_dag_run", setup.REQUIRED_TABLES)
        self.assertIn("tb_mig_src_layout", setup.REQUIRED_TABLES)
        self.assertNotIn("tb_mig_sbj_dep", setup.REQUIRED_TABLES)
        self.assertEqual(setup.schema_name("migration_meta"), "migration_meta")
        with self.assertRaises(ValueError):
            setup.schema_name("migration-meta")

    def test_mapping_and_type_contract(self) -> None:
        row = mapping.defaults({"PRJ_CD": "PRJ1", "SBJ_AREA_CD": "A010001", "SRC_SCH_NM": "SRC", "SRC_TBL_NM": "고객", "TGT_SCH_NM": "DWH", "TGT_TBL_NM": "DIM_CUSTOMER"})
        self.assertEqual(row["LOAD_STS_CD"], "FULL")
        self.assertEqual(row["PARL_MTHD_CD"], "NONE")
        self.assertEqual(data_type.redshift_type("bpchar", "12", 3), "VARCHAR(36)")
        self.assertEqual(data_type.redshift_type("numeric(18,2)"), "DECIMAL(18,2)")
        self.assertEqual(data_type.redshift_type("boolean"), "BOOLEAN")
        self.assertEqual(data_type.redshift_type("date"), "TIMESTAMP")

    def test_subject_and_table_dag_generation(self) -> None:
        settings = {"s3_default": 2, "s3_maximum": 4, "ins_default": 1, "ins_maximum": 1, "incr_schedule": "DLY_0200"}
        sources = dag_generator.area_dag_sources("A010001", settings)
        table_sources = dag_generator.table_dag_sources({"mpg_id": 101, "sbj_area_cd": "A010001"}, settings, "INCR")
        self.assertEqual(set(sources), {"mig_a010001_full_src_s3", "mig_a010001_full_s3_tgt", "mig_a010001_vald_src_s3", "mig_a010001_vald_s3_tgt"})
        self.assertEqual(set(table_sources), {"mig_a010001_101_incr_src_s3", "mig_a010001_101_incr_s3_tgt", "mig_a010001_101_incr_all"})
        for name, source in sources.items() | table_sources.items():
            compile(source, f"{name}.py", "exec")
            self.assertIn("tb_mig_dag_run", source)
            self.assertNotIn("TriggerDagRunOperator", source)
            self.assertNotIn("project_source", source)
        self.assertIn("VALIDATE_SRC_S3", table_sources["mig_a010001_101_incr_all"])
        self.assertIn("VALIDATE_S3_TGT", table_sources["mig_a010001_101_incr_all"])
        self.assertIn("schedule='0 2 * * *'", table_sources["mig_a010001_101_incr_all"])
        self.assertIn("load_sts_cd = 'FULL'", sources["mig_a010001_full_src_s3"])
        self.assertIn("load_sts_cd = 'INCR'", table_sources["mig_a010001_101_incr_src_s3"])
        self.assertIn("mig_metadata_schema", sources["mig_a010001_full_src_s3"])
        self.assertIn("DELETE FROM", table_sources["mig_a010001_101_incr_s3_tgt"])
        self.assertIn("TRUNCATE TABLE", table_sources["mig_a010001_101_incr_s3_tgt"])
        self.assertIn("tgt_load_sql", table_sources["mig_a010001_101_incr_s3_tgt"])
        self.assertIn("write_s3_manifest", table_sources["mig_a010001_101_incr_all"])
        self.assertIn("s3_manf_id", table_sources["mig_a010001_101_incr_all"])
        self.assertIn("source_dag_run_id", table_sources["mig_a010001_101_incr_s3_tgt"])
        self.assertIn("src_extract_sql", table_sources["mig_a010001_101_incr_src_s3"])
        self.assertIn("source_target_increment_columns", table_sources["mig_a010001_101_incr_all"])
        self.assertNotIn("MERGE INTO", table_sources["mig_a010001_101_incr_s3_tgt"])

    def test_source_and_target_sql_contract(self) -> None:
        settings = {"s3_default": 2, "s3_maximum": 4, "ins_default": 1, "ins_maximum": 1, "incr_schedule": "DLY_0200"}
        source = dag_generator.table_dag_sources({"mpg_id": 101, "sbj_area_cd": "A010001"}, settings, "INCR")["mig_a010001_101_incr_all"]
        self.assertIn('quote_identifier(record["src_sch_nm"])', source)
        self.assertIn('quote_identifier(record["src_tbl_nm"])', source)
        self.assertIn('quote_identifier(record["tgt_sch_nm"])', source)
        self.assertIn('quote_identifier(record["tgt_tbl_nm"])', source)
        self.assertIn('row["src_col_nm"]', source)
        self.assertIn('row["tgt_col_nm"]', source)
        self.assertIn('record["src_extract_sql"]', source)
        self.assertIn('record["tgt_load_sql"]', source)
        self.assertIn('src_where_cnd', source)
        self.assertIn('s3_manf_id', source)
        self.assertIn('TRUNCATE TABLE', source)
        self.assertIn('DELETE FROM', source)

        pendulum_module = types.ModuleType("pendulum")
        pendulum_module.datetime = lambda *args, **kwargs: None
        airflow_module = types.ModuleType("airflow")
        decorators_module = types.ModuleType("airflow.decorators")
        models_module = types.ModuleType("airflow.models")
        providers_module = types.ModuleType("airflow.providers")
        postgres_module = types.ModuleType("airflow.providers.postgres")
        hooks_module = types.ModuleType("airflow.providers.postgres.hooks")
        postgres_hook_module = types.ModuleType("airflow.providers.postgres.hooks.postgres")
        trigger_rule_module = types.ModuleType("airflow.utils.trigger_rule")

        def dag_decorator(*args, **kwargs):
            return lambda function: lambda *call_args, **call_kwargs: None

        def task_decorator(function=None, **kwargs):
            return function if callable(function) else lambda wrapped: wrapped

        decorators_module.dag = dag_decorator
        decorators_module.task = task_decorator
        models_module.Variable = type("Variable", (), {"get": staticmethod(lambda *args, **kwargs: "")})
        postgres_hook_module.PostgresHook = object
        trigger_rule_module.TriggerRule = type("TriggerRule", (), {"ALL_DONE": "ALL_DONE"})
        modules = {
            "pendulum": pendulum_module,
            "airflow": airflow_module,
            "airflow.decorators": decorators_module,
            "airflow.models": models_module,
            "airflow.providers": providers_module,
            "airflow.providers.postgres": postgres_module,
            "airflow.providers.postgres.hooks": hooks_module,
            "airflow.providers.postgres.hooks.postgres": postgres_hook_module,
            "airflow.utils.trigger_rule": trigger_rule_module,
        }
        namespace: dict[str, object] = {"__name__": "virtual_generated_dag"}
        with patch.dict(sys.modules, modules):
            exec(compile(source, "virtual_generated_dag.py", "exec"), namespace)
        column_rows = [
            {"src_col_nm": "CUST_NO", "tgt_col_nm": "CUSTOMER_ID", "trnsf_expr": None, "dflt_expr": None},
            {"src_col_nm": "RSN_CD", "tgt_col_nm": "REASON_CODE", "trnsf_expr": None, "dflt_expr": None},
            {"src_col_nm": "CRE_DTM", "tgt_col_nm": "CREATED_AT", "trnsf_expr": None, "dflt_expr": None},
            {"src_col_nm": "UPD_DTM", "tgt_col_nm": "UPDATED_AT", "trnsf_expr": None, "dflt_expr": None},
        ]
        namespace["column_mappings"] = lambda record: column_rows
        record = {
            "mpg_id": 101,
            "src_sch_nm": "GP_STAGE",
            "src_tbl_nm": "고객원장",
            "tgt_sch_nm": "DWH",
            "tgt_tbl_nm": "DIM_CUSTOMER",
            "load_sts_cd": "INCR",
            "sys_col_nm_arr": '["CRE_DTM", "UPD_DTM"]',
            "sys_col_fmt_cd": "YYYYMMDDHH24MISS",
            "src_incr_col_nm_arr": '["CUST_NO", "RSN_CD"]',
        }
        source_plan = namespace["source_extract_plan"](dict(record), {"sys_ref_val": "20260101000000"})
        source_plan = namespace["source_sql"](namespace["parallel_records"]([source_plan])[0])
        target_plan = namespace["target_load_plan"](dict(record))
        self.assertIn('FROM "GP_STAGE"."고객원장" AS S', source_plan["src_extract_sql"])
        self.assertIn('(S."CUST_NO", S."RSN_CD") IN (SELECT (I."CUST_NO", I."RSN_CD")', source_plan["src_extract_sql"])
        self.assertNotIn('DIM_CUSTOMER', source_plan["src_extract_sql"])
        self.assertIn('DELETE FROM "DWH"."DIM_CUSTOMER" AS T', target_plan["tgt_load_sql"])
        self.assertIn('S."CUST_NO" AS "CUSTOMER_ID", S."RSN_CD" AS "REASON_CODE"', target_plan["tgt_load_sql"])
        self.assertIn('INSERT INTO "DWH"."DIM_CUSTOMER" ("CUSTOMER_ID", "REASON_CODE", "CREATED_AT", "UPDATED_AT")', target_plan["tgt_load_sql"])
        self.assertNotIn('"고객원장"', target_plan["tgt_load_sql"])

    def test_parallel_and_state_contract(self) -> None:
        result = load_state.normalize_parallel("WHERE", '["abc_dt BETWEEN \'19000101\' AND \'20001231\'"]')
        self.assertEqual(result["count"], 1)
        with self.assertRaises(ValueError):
            load_state.normalize_parallel("RANGE", "[]")
        plan = load_state.transition_plan("FULL", "INCR", 1, False, '["CRE_DTM", "UPD_DTM"]', "TIMESTAMP", "PK_MERGE", '["PK1", "PK2"]')
        self.assertEqual(plan["runtime_method"], "INCR")

    def test_s3_failure_prevents_insert(self) -> None:
        module_name = "virtual_failed_executor"
        module = types.ModuleType(module_name)
        calls: list[str] = []

        def fail_s3(record):
            calls.append("S3")
            raise RuntimeError("원천 추출 실패")

        module.run_s3 = fail_s3
        module.run_ins = lambda record: calls.append("INS")
        sys.modules[module_name] = module
        logs: list[tuple[str, str]] = []
        with self.assertRaisesRegex(RuntimeError, "원천 추출 실패"):
            execute_logged_step({"mpg_id": 1}, "S3", "원천 S3 적재", module_name, lambda record, step, status, message: logs.append((step, status)))
        self.assertEqual(calls, ["S3"])
        self.assertEqual(logs, [("S3", "RUNNING"), ("S3", "FAILED")])

    def test_artifact_style_contract(self) -> None:
        data = artifact.excel_bytes([("테스트", pd.DataFrame({"항목": ["값"]}))])
        book = load_workbook(BytesIO(data))
        sheet = book["테스트"]
        self.assertEqual(sheet.freeze_panes, "A2")
        self.assertTrue(sheet["A1"].font.bold)
        self.assertEqual(sheet["A1"].font.name, "맑은 고딕")
        self.assertEqual(sheet["A1"].fill.fgColor.rgb[-6:], "FFF2CC")

    def test_redshift_comment_literal_contract(self) -> None:
        table = pd.Series({"tgt_sch_nm": "dwh", "tgt_tbl_nm": "customer", "tgt_tbl_cmt": "고객\n설명 '확인'"})
        columns = pd.DataFrame([{"tgt_col_nm": "customer_name", "tgt_col_cmt": "고객명\n한글", "tgt_data_type": "varchar(100)", "tgt_null_yn": True, "dflt_expr": None}])
        ddl = target_reflection.ddl_for(table, columns, {"dist_style": "AUTO", "dist_key": "", "sort_style": "AUTO", "sort_cols": "", "encd_auto": True})
        self.assertIn("COMMENT ON TABLE", ddl)
        self.assertIn("고객\n설명 ''확인''", ddl)
        self.assertIn("COMMENT ON COLUMN", ddl)

    def test_screen_structure_contract(self) -> None:
        control = (PROJECT_ROOT / "app" / "SrcTgtControl.py").read_text(encoding="utf-8")
        orchestration = (PROJECT_ROOT / "app" / "SrcTgtOrchestrator.py").read_text(encoding="utf-8")
        self.assertIn('"🔌 접속정보", "🗂️ 주제영역", "🔗 SRC·TGT 매핑", "⚙️ DAG 생성", "✅ 검증", "📋 실행 이력", "📦 산출물"', control)
        self.assertNotIn("프로젝트 오케스트레이터", control)
        self.assertIn('title="초기 설정"', orchestration)
        self.assertIn('title="실행 현황"', orchestration)
        self.assertTrue((PROJECT_ROOT / "app" / "SrcTgtInitialize.py").exists())


if __name__ == "__main__":
    unittest.main()

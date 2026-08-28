from __future__ import annotations

import sys
import tempfile
import types
import unittest
from datetime import datetime
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
import SrcTgtAirflow as airflow
import SrcTgtDagGenerator as dag_generator
import SrcTgtDataType as data_type
import SrcTgtLoadState as load_state
import SrcTgtMapping as mapping
import SrcTgtSetup as setup
import SrcTgtTargetReflection as target_reflection
from common.mig_emr_runtime import terminate_profile
from common.mig_step_runtime import execute_logged_step


class VirtualWorkflowTest(unittest.TestCase):
    def test_metadata_contract(self) -> None:
        ddl = (PROJECT_ROOT / "sql" / "01_mig_metadata_ddl.sql").read_text(encoding="utf-8").upper()
        subject_block = ddl.split("CREATE TABLE MIG_META.TB_MIG_SBJ_AREA", 1)[1].split("CREATE TABLE MIG_META.TB_MIG_SBJ_DAG_MPG", 1)[0]
        table_block = ddl.split("CREATE TABLE MIG_META.TB_MIG_TBL_MPG", 1)[1].split("CREATE TABLE MIG_META.TB_MIG_COL_MPG", 1)[0]
        column_block = ddl.split("CREATE TABLE MIG_META.TB_MIG_COL_MPG", 1)[1].split("CREATE TABLE MIG_META.TB_MIG_MPG_CHG_HIST", 1)[0]
        for name in ("TB_MIG_CONN", "TB_MIG_AIRFLOW", "TB_MIG_EMR", "TB_MIG_SBJ_AREA", "TB_MIG_SBJ_DAG_MPG", "TB_MIG_DAG_DPLY_HIST", "TB_MIG_EMR_RUN", "TB_MIG_SRC_LAYOUT", "TB_MIG_TBL_MPG", "TB_MIG_COL_MPG", "TB_MIG_MPG_CHG_HIST", "TB_MIG_S3_MANF", "TB_MIG_DAG_RUN", "TB_MIG_RUN_LOG", "TB_MIG_VALD_RSLT", "TB_MIG_VALD_COL_RSLT", "TB_MIG_TBL_LOAD_HIST", "TB_MIG_ARTF_ITEM"):
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
        self.assertIn("COL_MPG_MTHD_CD", ddl)
        self.assertIn("SRC_REF_COL_NM_ARR", ddl)
        self.assertNotIn("S3_COL_NM", ddl)
        self.assertNotIn("SRC_EXPR", ddl)
        self.assertIn("TGT_EXPR", ddl)
        self.assertNotIn("TRNSF_EXPR", ddl)
        self.assertIn("SRC_EXT_SQL", ddl)
        self.assertIn("TGT_LOAD_SQL", ddl)
        self.assertNotIn("BASIS_STT_VAL", ddl)
        self.assertIn("SRC_CONN_ID", subject_block)
        self.assertIn("TGT_CONN_ID", subject_block)
        self.assertIn("UP_SBJ_AREA_CD", subject_block)
        self.assertNotIn("SRC_CONN_ID", table_block)
        self.assertNotIn("TGT_CONN_ID", table_block)
        self.assertLess(column_block.index("TGT_COL_NM"), column_block.index("SRC_COL_NM"))
        self.assertLess(column_block.index("COL_MPG_MTHD_CD"), column_block.index("SRC_COL_NM"))
        self.assertIn("AIRFLOW_ID", ddl)
        self.assertIn("EMR_ID", ddl)

    def test_redshift_ddl_static_parse(self) -> None:
        source = (PROJECT_ROOT / "sql" / "01_mig_metadata_ddl.sql").read_text(encoding="utf-8")
        expressions = sqlglot.parse(source, read="redshift")
        self.assertGreater(len(expressions), 20)

    def test_initial_setup_contract(self) -> None:
        self.assertIn("tb_mig_dag_run", setup.REQUIRED_TABLES)
        self.assertIn("tb_mig_airflow", setup.REQUIRED_TABLES)
        self.assertIn("tb_mig_emr", setup.REQUIRED_TABLES)
        self.assertIn("tb_mig_src_layout", setup.REQUIRED_TABLES)
        self.assertNotIn("tb_mig_sbj_dep", setup.REQUIRED_TABLES)
        self.assertEqual(setup.schema_name("migration_meta"), "migration_meta")
        with self.assertRaises(ValueError):
            setup.schema_name("migration-meta")

    def test_mapping_and_type_contract(self) -> None:
        row = mapping.defaults({"PRJ_CD": "PRJ1", "SBJ_AREA_CD": "A010001", "SRC_SCH_NM": "SRC", "SRC_TBL_NM": "고객", "TGT_SCH_NM": "DWH", "TGT_TBL_NM": "DIM_CUSTOMER"})
        self.assertEqual(row["LOAD_STS_CD"], "FULL")
        self.assertEqual(row["PARL_MTHD_CD"], "NONE")
        self.assertEqual(load_state.normalize_name_array("CRE_DTM, UPD_DTM", "시스템컬럼명"), ["CRE_DTM", "UPD_DTM"])
        self.assertEqual(load_state.normalize_name_array("PK1 PK2", "증분컬럼명"), ["PK1", "PK2"])
        self.assertLess(mapping.TABLE_FIELDS.index("TGT_TBL_NM"), mapping.TABLE_FIELDS.index("SRC_TBL_NM"))
        self.assertLess(mapping.COLUMN_DETAIL_FIELDS.index("TGT_COL_NM"), mapping.COLUMN_DETAIL_FIELDS.index("SRC_COL_NM"))
        self.assertEqual(data_type.redshift_type("bpchar", "12", 3), "VARCHAR(36)")
        self.assertEqual(data_type.redshift_type("numeric(18,2)"), "DECIMAL(18,2)")
        self.assertEqual(data_type.redshift_type("boolean"), "BOOLEAN")
        self.assertEqual(data_type.redshift_type("date"), "TIMESTAMP")
        base = {field: None for field in mapping.COLUMN_FIELDS}
        base.update({"MPG_ID": 1, "COL_ORD": 1, "TGT_COL_NM": "LOAD_DVSN", "TGT_DATA_TYPE": "VARCHAR(1)", "COL_MPG_MTHD_CD": "CONST", "TGT_EXPR": "'I'"})
        constant = mapping.normalized_columns(pd.DataFrame([base]))[0]
        self.assertEqual(constant["COL_MPG_MTHD_CD"], "CONST")
        self.assertIsNone(constant["SRC_REF_COL_NM_ARR"])
        null_row = dict(base) | {"COL_ORD": 2, "TGT_COL_NM": "OPTIONAL_VAL", "COL_MPG_MTHD_CD": "NULL", "TGT_EXPR": None}
        self.assertEqual(mapping.normalized_columns(pd.DataFrame([null_row]))[0]["COL_MPG_MTHD_CD"], "NULL")
        move_row = dict(base) | {"COL_ORD": 3, "TGT_COL_NM": "CUSTOMER_ID", "COL_MPG_MTHD_CD": "MOVE", "TGT_EXPR": None}
        with self.assertRaisesRegex(ValueError, "원천컬럼명"):
            mapping.normalized_columns(pd.DataFrame([move_row]))
        table = pd.Series({"src_sch_nm": "SRC", "src_tbl_nm": "CUSTOMER", "tgt_sch_nm": "DWH", "tgt_tbl_nm": "DIM_CUSTOMER", "load_sts_cd": "INCR", "src_incr_col_nm_arr": '["CUST_NO"]'})
        sql_rows = pd.DataFrame([
            {"COL_ORD": 1, "TGT_COL_NM": "CUSTOMER_ID", "TGT_DATA_TYPE": "VARCHAR(30)", "COL_MPG_MTHD_CD": "MOVE", "TGT_EXPR": None, "DFLT_EXPR": None, "SRC_REF_COL_NM_ARR": '["CUST_NO"]', "SRC_COL_NM": "CUST_NO"},
            {"COL_ORD": 2, "TGT_COL_NM": "LOAD_DVSN", "TGT_DATA_TYPE": "VARCHAR(1)", "COL_MPG_MTHD_CD": "CONST", "TGT_EXPR": "'I'", "DFLT_EXPR": None, "SRC_REF_COL_NM_ARR": None, "SRC_COL_NM": None},
            {"COL_ORD": 3, "TGT_COL_NM": "OPTIONAL_VAL", "TGT_DATA_TYPE": "VARCHAR(10)", "COL_MPG_MTHD_CD": "NULL", "TGT_EXPR": None, "DFLT_EXPR": None, "SRC_REF_COL_NM_ARR": None, "SRC_COL_NM": None},
            {"COL_ORD": 4, "TGT_COL_NM": "CUSTOMER_LABEL", "TGT_DATA_TYPE": "VARCHAR(100)", "COL_MPG_MTHD_CD": "EXPR", "TGT_EXPR": "CONCAT(S.\"CUST_NO\", '_', S.\"CUST_NM\")", "DFLT_EXPR": None, "SRC_REF_COL_NM_ARR": '["CUST_NO", "CUST_NM"]', "SRC_COL_NM": None},
        ])
        source_sql, target_sql = mapping.sql_templates(table, sql_rows)
        self.assertIn('S."CUST_NO" AS "CUST_NO"', source_sql)
        self.assertIn("__SRC_WHERE_CND__", source_sql)
        self.assertIn('S."CUST_NO"', target_sql)
        self.assertIn("CAST(NULL AS VARCHAR(10))", target_sql)
        self.assertIn("__MIG_STAGE__", target_sql)
        mapping.validate_sql_pair(source_sql, target_sql, sql_rows)
        mapping.validate_sql_pair('SELECT S."CUST_NO" FROM "SRC"."CUSTOMER" AS S WHERE __SRC_WHERE_CND__', target_sql, sql_rows)

    def test_subject_and_table_dag_generation(self) -> None:
        settings = {"s3_default": 2, "s3_maximum": 4, "ins_default": 1, "ins_maximum": 1, "incr_schedule": "DLY_0200"}
        sources = dag_generator.area_dag_sources("A010001", settings)
        table_sources = dag_generator.table_dag_sources({"mpg_id": 101, "sbj_area_cd": "A010001"}, settings, "INCR")
        self.assertEqual(set(sources), {"mig_a010001_full_src_s3", "mig_a010001_full_s3_tgt", "mig_a010001_full_all", "mig_a010001_vald_src_s3", "mig_a010001_vald_s3_tgt"})
        self.assertEqual(set(table_sources), {"mig_a010001_101_incr_src_s3", "mig_a010001_101_incr_s3_tgt", "mig_a010001_101_incr_all"})
        for name, source in sources.items() | table_sources.items():
            compile(source, f"{name}.py", "exec")
            self.assertIn("tb_mig_dag_run", source)
            self.assertNotIn("TriggerDagRunOperator", source)
            self.assertNotIn("project_source", source)
        self.assertIn("VALIDATE_SRC_S3", table_sources["mig_a010001_101_incr_all"])
        self.assertIn("VALIDATE_S3_TGT", table_sources["mig_a010001_101_incr_all"])
        self.assertIn("VALIDATE_SRC_S3", sources["mig_a010001_full_all"])
        self.assertIn("VALIDATE_S3_TGT", sources["mig_a010001_full_all"])
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
        emr_sources = dag_generator.area_dag_sources("A010001", settings | {"emr_id": "EMR_LOAD"})
        self.assertIn("EMR_ID = 'EMR_LOAD'", emr_sources["mig_a010001_full_all"])
        self.assertIn("terminate_emr", emr_sources["mig_a010001_full_all"])
        self.assertIn("EMR_ID = None", emr_sources["mig_a010001_full_src_s3"])

    def test_airflow_deployment_and_emr_contract(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            paths = airflow.write_shared_sources({"dag_deploy_root": directory}, {"mig_test": "value = 1\n"})
            self.assertEqual(len(paths), 1)
            self.assertEqual(Path(paths[0]).read_text(encoding="utf-8"), "value = 1\n")
        with self.assertRaises(ValueError):
            airflow.safe_dag_name("../unsafe")
        dedicated = {"emr_id": "EMR1", "emr_type_cd": "EMR_EC2", "aws_conn_id": "aws_default", "emr_cluster_id": "j-123", "dedicated_yn": True, "auto_term_yn": True}
        client_calls: list[dict[str, object]] = []

        class FakeHook:
            def __init__(self, aws_conn_id: str):
                self.aws_conn_id = aws_conn_id

            def get_client_type(self, value: str):
                return type("Client", (), {"terminate_job_flows": lambda _, **kwargs: client_calls.append(kwargs)})()

        amazon_module = types.ModuleType("airflow.providers.amazon")
        aws_module = types.ModuleType("airflow.providers.amazon.aws")
        hooks_module = types.ModuleType("airflow.providers.amazon.aws.hooks")
        base_module = types.ModuleType("airflow.providers.amazon.aws.hooks.base_aws")
        base_module.AwsBaseHook = FakeHook
        with patch.dict(sys.modules, {"airflow.providers.amazon": amazon_module, "airflow.providers.amazon.aws": aws_module, "airflow.providers.amazon.aws.hooks": hooks_module, "airflow.providers.amazon.aws.hooks.base_aws": base_module}):
            terminate_profile(dedicated)
        self.assertEqual(client_calls, [{"JobFlowIds": ["j-123"]}])
        with self.assertRaises(ValueError):
            airflow.deployment_root({"dag_deploy_root": "C:\\"})

    def test_source_and_target_sql_contract(self) -> None:
        settings = {"s3_default": 2, "s3_maximum": 4, "ins_default": 1, "ins_maximum": 1, "incr_schedule": "DLY_0200"}
        source = dag_generator.table_dag_sources({"mpg_id": 101, "sbj_area_cd": "A010001"}, settings, "INCR")["mig_a010001_101_incr_all"]
        self.assertIn('quote_identifier(record["src_sch_nm"])', source)
        self.assertIn('quote_identifier(record["src_tbl_nm"])', source)
        self.assertIn('quote_identifier(record["tgt_sch_nm"])', source)
        self.assertIn('quote_identifier(record["tgt_tbl_nm"])', source)
        self.assertIn('source_layout_columns', source)
        self.assertIn('row["tgt_col_nm"]', source)
        self.assertIn('record["src_extract_sql"]', source)
        self.assertIn('record["tgt_load_sql"]', source)
        self.assertIn('src_ext_sql', source)
        self.assertIn('tgt_load_sql', source)
        self.assertIn('__SRC_WHERE_CND__', source)
        self.assertIn('__MIG_STAGE__', source)
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
            {"src_col_nm": "CUST_NO", "tgt_col_nm": "CUSTOMER_ID", "tgt_data_type": "VARCHAR(30)", "col_mpg_mthd_cd": "MOVE", "tgt_expr": None, "dflt_expr": None, "src_ref_col_nm_arr": '["CUST_NO"]'},
            {"src_col_nm": "RSN_CD", "tgt_col_nm": "REASON_CODE", "tgt_data_type": "VARCHAR(10)", "col_mpg_mthd_cd": "MOVE", "tgt_expr": None, "dflt_expr": None, "src_ref_col_nm_arr": '["RSN_CD"]'},
            {"src_col_nm": "CRE_DTM", "tgt_col_nm": "CREATED_AT", "tgt_data_type": "TIMESTAMP", "col_mpg_mthd_cd": "MOVE", "tgt_expr": None, "dflt_expr": None, "src_ref_col_nm_arr": '["CRE_DTM"]'},
            {"src_col_nm": "UPD_DTM", "tgt_col_nm": "UPDATED_AT", "tgt_data_type": "TIMESTAMP", "col_mpg_mthd_cd": "MOVE", "tgt_expr": None, "dflt_expr": None, "src_ref_col_nm_arr": '["UPD_DTM"]'},
            {"src_col_nm": None, "tgt_col_nm": "LOAD_DVSN", "tgt_data_type": "VARCHAR(1)", "col_mpg_mthd_cd": "CONST", "tgt_expr": "'I'", "dflt_expr": None, "src_ref_col_nm_arr": None},
            {"src_col_nm": None, "tgt_col_nm": "OPTIONAL_VAL", "tgt_data_type": "VARCHAR(10)", "col_mpg_mthd_cd": "NULL", "tgt_expr": None, "dflt_expr": None, "src_ref_col_nm_arr": None},
            {"src_col_nm": None, "tgt_col_nm": "CUSTOMER_REASON", "tgt_data_type": "VARCHAR(50)", "col_mpg_mthd_cd": "EXPR", "tgt_expr": "CONCAT(S.\"CUST_NO\", '_', S.\"RSN_CD\")", "dflt_expr": None, "src_ref_col_nm_arr": '["CUST_NO", "RSN_CD"]'},
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
            "s3_stg_path": "s3://migration-stage",
            "s3_rlt_path": "dwh__dim_customer",
            "dag_run_id": "manual__2026-08-27T00:00:00+00:00",
        }
        source_plan = namespace["source_extract_plan"](dict(record), {"sys_ref_val": "20260101000000"})
        source_plan = namespace["source_sql"](namespace["parallel_records"]([source_plan])[0])
        target_plan = namespace["target_load_plan"](dict(record))
        self.assertIn('FROM "GP_STAGE"."고객원장" AS S', source_plan["src_extract_sql"])
        self.assertIn('(S."CUST_NO", S."RSN_CD") IN (SELECT (I."CUST_NO", I."RSN_CD")', source_plan["src_extract_sql"])
        self.assertNotIn("CUSTOMER_REASON", source_plan["src_extract_sql"])
        self.assertNotIn('DIM_CUSTOMER', source_plan["src_extract_sql"])
        self.assertEqual(source_plan["s3_load_path"], f"s3://migration-stage/incr/dwh__dim_customer/wrk_dt={datetime.now().strftime('%Y%m%d')}/run_id=manual__2026-08-27T00_00_00_00_00")
        self.assertEqual(source_plan["s3_retention_days"], 31)
        full_s3_plan = namespace["source_extract_plan"](dict(record) | {"load_sts_cd": "FULL"}, {"wrk_dt": "20260827"})
        self.assertEqual(full_s3_plan["s3_load_path"], "s3://migration-stage/full/dwh__dim_customer")
        self.assertTrue(full_s3_plan["s3_cleanup_before_write"])
        self.assertIn('DELETE FROM "DWH"."DIM_CUSTOMER" AS T', target_plan["tgt_load_sql"])
        self.assertIn('S."CUST_NO" AS "CUSTOMER_ID", S."RSN_CD" AS "REASON_CODE"', target_plan["tgt_load_sql"])
        self.assertIn('INSERT INTO "DWH"."DIM_CUSTOMER" ("CUSTOMER_ID", "REASON_CODE", "CREATED_AT", "UPDATED_AT", "LOAD_DVSN", "OPTIONAL_VAL", "CUSTOMER_REASON")', target_plan["tgt_load_sql"])
        self.assertIn("CAST(NULL AS VARCHAR(10))", target_plan["tgt_load_sql"])
        self.assertIn("CONCAT(S.\"CUST_NO\", '_', S.\"RSN_CD\")", target_plan["tgt_load_sql"])
        self.assertNotIn('"고객원장"', target_plan["tgt_load_sql"])
        source_custom = dict(record) | {"src_ext_sql": 'SELECT S."CUST_NO", L."CUST_GRP" FROM "GP_STAGE"."고객원장" AS S LEFT JOIN "GP_STAGE"."고객분류" AS L ON L."CUST_NO" = S."CUST_NO" WHERE __SRC_WHERE_CND__'}
        source_custom_plan = namespace["source_sql"](namespace["parallel_records"]([namespace["source_extract_plan"](source_custom, {"sys_ref_val": "20260101000000"})])[0])
        self.assertIn('LEFT JOIN "GP_STAGE"."고객분류"', source_custom_plan["src_extract_sql"])
        self.assertNotIn("__SRC_WHERE_CND__", source_custom_plan["src_extract_sql"])
        target_custom = dict(record) | {"tgt_load_sql": 'DELETE FROM __TGT_TABLE__ AS T USING __MIG_STAGE__ AS S WHERE T."CUSTOMER_ID" = S."CUST_NO"; INSERT INTO __TGT_TABLE__ SELECT * FROM __MIG_STAGE__'}
        target_custom_plan = namespace["target_load_plan"](target_custom)
        self.assertIn('DELETE FROM "DWH"."DIM_CUSTOMER"', target_custom_plan["tgt_load_sql"])
        self.assertIn("__MIG_STAGE__", target_custom_plan["tgt_load_sql"])

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
        self.assertIn('st.subheader(":material/link: SRC·TGT 매핑")', control)
        self.assertNotIn('"☁️ Airflow"', control)
        self.assertIn('st.switch_page("SrcTgtTargetDdl.py"', mapping_source := (PROJECT_ROOT / "app" / "SrcTgtMapping.py").read_text(encoding="utf-8"))
        self.assertNotIn("프로젝트 오케스트레이터", control)
        self.assertIn('"기준정보"', orchestration)
        self.assertIn('title="테이블 레이아웃"', orchestration)
        self.assertIn('title="SRC·TGT 매핑"', orchestration)
        self.assertIn('title="DAG 생성"', orchestration)
        self.assertIn('title="초기 설정"', orchestration)
        self.assertIn('title="실행 현황"', orchestration)
        self.assertTrue((PROJECT_ROOT / "app" / "SrcTgtInitialize.py").exists())
        self.assertTrue((PROJECT_ROOT / "app" / "SrcTgtReference.py").exists())
        self.assertTrue((PROJECT_ROOT / "app" / "SrcTgtDagManagement.py").exists())
        self.assertTrue((PROJECT_ROOT / "app" / "SrcTgtValidationManagement.py").exists())
        self.assertTrue((PROJECT_ROOT / "app" / "SrcTgtArtifactManagement.py").exists())
        self.assertIn('"S3 이관 SQL", "INS 이행 SQL"', mapping_source)
        self.assertNotIn('mapping_tab, sql_tab, transition_tab, upload_tab', mapping_source)
        self.assertIn("restore_sql_history", mapping_source)
        self.assertIn("before_value", mapping_source)
        self.assertIn('st.switch_page("SrcTgtTargetDdl.py"', mapping_source)
        self.assertIn('st.switch_page("SrcTgtLayoutHistory.py"', mapping_source)
        layout_source = (PROJECT_ROOT / "app" / "SrcTgtLayoutHistory.py").read_text(encoding="utf-8")
        self.assertIn("def captured_tables", layout_source)
        self.assertIn('"src_conn_id": connection_id', layout_source)
        self.assertIn('st.switch_page("SrcTgtControl.py"', layout_source)
        self.assertIn('"src_sch_nm"', layout_source)


if __name__ == "__main__":
    unittest.main()

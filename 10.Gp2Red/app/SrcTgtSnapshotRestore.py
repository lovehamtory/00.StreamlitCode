from __future__ import annotations

import json
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from SrcTgtConnection import connection_frame
from SrcTgtRuntime import qualified, query_frame, runtime_context, text


TABLE_COLUMNS = ["source_schema", "source_table"]
RECORD_COLUMNS = ["순서", "원본", "대상", "상태", "진행률", "경과 시간", "요청 ID", "메시지", "갱신 시각"]
TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELED"}
ACTIVE_STATES = {"QUEUED", "PENDING", "IN_PROGRESS", "WAIT_TIMEOUT"}
SNAPSHOT_TYPES = {"전체": None, "자동": "automated", "수동": "manual"}
RESTORE_MODES = {"별도 테이블 복구": "copy", "원본 DROP 후 복구": "replace"}
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUN_STORE = PROJECT_ROOT / "log" / "snapshot_restore_runs"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def empty_table_frame() -> pd.DataFrame:
    return pd.DataFrame({column: pd.Series([""], dtype="string") for column in TABLE_COLUMNS})


def initialize_state() -> None:
    RUN_STORE.mkdir(parents=True, exist_ok=True)
    st.session_state.setdefault("restore_table_draft", empty_table_frame())
    st.session_state.setdefault("snapshots", [])
    st.session_state.setdefault("snapshot_context", "")
    st.session_state.setdefault("restore_records", [])
    st.session_state.setdefault("active_run_id", "")
    st.session_state.setdefault("active_run_context", {})
    st.session_state.setdefault("target_access_checks", {})
    st.session_state.setdefault("drop_preflight", {})


def get_redshift_settings(section_name: str) -> dict[str, str]:
    if not section_name or section_name not in st.secrets:
        return {}
    return {
        key: str(value)
        for key, value in dict(st.secrets[section_name]).items()
        if value is not None
    }


def get_database_options(context: Any) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    connections = connection_frame(query_frame, context.values, context.schema_name, qualified, active_only=True)
    candidates = connections.loc[connections.dbms_cd.map(text).str.upper().eq("REDSHIFT")]
    for row in candidates.itertuples(index=False):
        section_name = text(row.sec_sect_nm)
        values = get_redshift_settings(section_name)
        database_name = values.get("target_database") or values.get("database") or values.get("database_name", "")
        options.append(
            {
                "conn_id": text(row.conn_id),
                "label": f"{text(row.conn_id)} · {text(row.conn_nm)}",
                "sec_sect_nm": section_name,
                "cluster_identifier": values.get("cluster_identifier", ""),
                "source_database": values.get("source_database") or database_name,
                "target_database": database_name,
                "default_schema": values.get("default_schema", "public"),
                "data_api_db_user": values.get("data_api_db_user", ""),
                "settings": values,
            }
        )
    return options


def setting_bool(settings: dict[str, str], key: str, default: bool) -> bool:
    value = settings.get(key)
    if value is None:
        return default
    return value.strip().lower() in {"true", "1", "yes", "y"}


def setting_int(settings: dict[str, str], key: str, default: int, minimum: int, maximum: int) -> int:
    try:
        return min(max(int(settings.get(key, default)), minimum), maximum)
    except ValueError:
        return default


def get_aws_session(settings: dict[str, str]) -> Any:
    try:
        import boto3
    except ModuleNotFoundError as error:
        raise RuntimeError("boto3가 설치되지 않았습니다. requirements.txt를 설치해 주십시오.") from error

    session_options: dict[str, str] = {}
    credential_keys = ["aws_access_key_id", "aws_secret_access_key", "aws_session_token"]
    supplied_credentials = {key: settings[key] for key in credential_keys if settings.get(key)}
    if supplied_credentials:
        if {"aws_access_key_id", "aws_secret_access_key"} - supplied_credentials.keys():
            raise RuntimeError("AWS 액세스 키와 비밀 키는 함께 설정해야 합니다.")
        session_options.update(supplied_credentials)
    else:
        profile_name = settings.get("aws_profile_name") or settings.get("profile_name")
        if profile_name:
            session_options["profile_name"] = profile_name
    if settings.get("region_name"):
        session_options["region_name"] = settings["region_name"]
    return boto3.Session(**session_options)


def get_redshift_client(settings: dict[str, str]) -> Any:
    return get_aws_session(settings).client("redshift")


def get_redshift_data_client(settings: dict[str, str]) -> Any:
    return get_aws_session(settings).client("redshift-data")


def validate_target_access(settings: dict[str, str], cluster_identifier: str) -> None:
    if not cluster_identifier.strip():
        raise RuntimeError("선택한 DB의 클러스터 식별자가 TOML에 설정되지 않았습니다.")
    get_redshift_client(settings).describe_cluster_snapshots(
        ClusterIdentifier=cluster_identifier.strip(),
        ClusterExists=True,
        MaxRecords=20,
    )


def snapshot_label(snapshot: dict[str, Any]) -> str:
    created_at = snapshot.get("SnapshotCreateTime")
    created_text = created_at.strftime("%Y-%m-%d %H:%M:%S") if isinstance(created_at, datetime) else "시간 정보 없음"
    return f"{snapshot.get('SnapshotIdentifier', '')} | {snapshot.get('SnapshotType', '')} | 생성일시: {created_text}"


def snapshot_created_at(snapshot: dict[str, Any]) -> datetime:
    created_at = snapshot.get("SnapshotCreateTime")
    if not isinstance(created_at, datetime):
        return datetime.min.replace(tzinfo=timezone.utc)
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at


def list_snapshots(client: Any, cluster_identifier: str, snapshot_type: str | None) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    marker: str | None = None
    while True:
        request: dict[str, Any] = {
            "ClusterIdentifier": cluster_identifier,
            "ClusterExists": True,
            "MaxRecords": 100,
        }
        if snapshot_type:
            request["SnapshotType"] = snapshot_type
        if marker:
            request["Marker"] = marker
        response = client.describe_cluster_snapshots(**request)
        snapshots.extend(response.get("Snapshots", []))
        marker = response.get("Marker")
        if not marker:
            break
    available = [snapshot for snapshot in snapshots if str(snapshot.get("Status", "")).lower() == "available"]
    return sorted(available, key=snapshot_created_at, reverse=True)


def validate_snapshot(client: Any, cluster_identifier: str, snapshot_identifier: str) -> dict[str, Any]:
    response = client.describe_cluster_snapshots(
        ClusterIdentifier=cluster_identifier,
        SnapshotIdentifier=snapshot_identifier,
        ClusterExists=True,
        MaxRecords=20,
    )
    snapshots = response.get("Snapshots", [])
    if len(snapshots) != 1:
        raise RuntimeError("선택한 스냅샷을 현재 클러스터에서 찾을 수 없습니다.")
    snapshot = snapshots[0]
    if str(snapshot.get("Status", "")).lower() != "available":
        raise RuntimeError("스냅샷 상태가 available이 아닙니다.")
    return snapshot


def canonical_column_name(value: Any) -> str:
    return str(value).strip().lower().replace(" ", "_")


def normalize_table_frame(frame: pd.DataFrame, default_schema: str) -> pd.DataFrame:
    aliases = {
        "source_schema": "source_schema",
        "원본_스키마": "source_schema",
        "source_table": "source_table",
        "원본_테이블": "source_table",
    }
    renamed = frame.rename(columns={column: aliases.get(canonical_column_name(column), canonical_column_name(column)) for column in frame.columns})
    normalized = pd.DataFrame(index=renamed.index)
    for column in TABLE_COLUMNS:
        normalized[column] = renamed[column] if column in renamed.columns else ""
        normalized[column] = normalized[column].fillna("").astype("string").str.strip()
    normalized["source_schema"] = normalized["source_schema"].mask(normalized["source_schema"] == "", default_schema)
    return normalized.reset_index(drop=True)


def valid_restore_tables(frame: pd.DataFrame, case_sensitive: bool) -> tuple[pd.DataFrame, list[str]]:
    result = frame.copy()
    for column in TABLE_COLUMNS:
        result[column] = result[column].fillna("").astype("string").str.strip()
    result = result[result["source_table"] != ""].reset_index(drop=True)
    errors: list[str] = []
    if result.empty:
        errors.append("복구할 원본 테이블을 한 건 이상 입력해 주십시오.")
    if result[TABLE_COLUMNS].eq("").any(axis=1).any():
        errors.append("스키마와 테이블명은 모두 입력해야 합니다.")
    duplicate_frame = result[["source_schema", "source_table"]].copy()
    if not case_sensitive:
        duplicate_frame = duplicate_frame.apply(lambda column: column.str.lower())
    if duplicate_frame.duplicated(keep=False).any():
        errors.append("동일한 원본 스키마와 테이블명은 한 번만 지정할 수 있습니다.")
    return result, errors


def make_restore_table_name(source_table: str, restore_mode: str, restore_suffix: str) -> str:
    if restore_mode == "replace":
        return source_table
    suffix = f"_{restore_suffix}"
    available_bytes = 127 - len(suffix.encode("utf-8"))
    source_prefix = source_table.encode("utf-8")[:available_bytes].decode("utf-8", errors="ignore")
    return f"{source_prefix}{suffix}"


def build_records(tables: pd.DataFrame, restore_mode: str, restore_suffix: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for sequence, row in tables.iterrows():
        source_schema = str(row.source_schema)
        source_table = str(row.source_table)
        records.append(
            {
                "sequence": int(sequence + 1),
                "source_schema": source_schema,
                "source_table": source_table,
                "target_schema": source_schema,
                "target_table": make_restore_table_name(source_table, restore_mode, restore_suffix),
                "status": "QUEUED",
                "progress": 0,
                "request_id": "",
                "message": "복구 요청 대기",
                "started_at": "",
                "finished_at": "",
                "updated_at": utc_now(),
            }
        )
    return records


def elapsed_time_text(record: dict[str, Any]) -> str:
    started_at = str(record.get("started_at") or "")
    if not started_at:
        return "-"
    try:
        started = datetime.fromisoformat(started_at)
        finished = datetime.fromisoformat(str(record.get("finished_at") or utc_now()))
    except ValueError:
        return "-"
    elapsed_seconds = max(0, int((finished - started).total_seconds()))
    hours, remainder = divmod(elapsed_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def records_frame(records: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for record in records:
        rows.append(
            {
                "순서": record["sequence"],
                "원본": f"{record['source_schema']}.{record['source_table']}",
                "대상": f"{record['target_schema']}.{record['target_table']}",
                "상태": record["status"],
                "진행률": record["progress"],
                "경과 시간": elapsed_time_text(record),
                "요청 ID": record["request_id"],
                "메시지": record["message"],
                "갱신 시각": record["updated_at"],
            }
        )
    return pd.DataFrame(rows, columns=RECORD_COLUMNS)


def display_restore_summary(placeholder: Any, records: list[dict[str, Any]]) -> None:
    total = len(records)
    succeeded = sum(record["status"] == "SUCCEEDED" for record in records)
    failed = sum(record["status"] in {"FAILED", "CANCELED"} for record in records)
    active = sum(record["status"] in {"PENDING", "IN_PROGRESS"} for record in records)
    queued = sum(record["status"] in {"QUEUED", "WAIT_TIMEOUT"} for record in records)
    with placeholder.container():
        with st.container(horizontal=True):
            st.metric("전체", total, border=True)
            st.metric("완료", succeeded, border=True)
            st.metric("진행 중", active, border=True)
            st.metric("대기·확인 필요", queued + failed, border=True)


def display_restore_records(placeholder: Any, records: list[dict[str, Any]]) -> None:
    placeholder.dataframe(
        records_frame(records),
        hide_index=True,
        column_config={
            "진행률": st.column_config.ProgressColumn("진행률", min_value=0, max_value=100, format="%d%%"),
            "경과 시간": st.column_config.TextColumn("경과 시간", width="small"),
            "순서": st.column_config.NumberColumn("순서", format="%d"),
        },
    )


def progress_percent(status: dict[str, Any]) -> int:
    total = status.get("TotalDataInMegaBytes") or 0
    progressed = status.get("ProgressInMegaBytes") or 0
    if total <= 0:
        return 100 if status.get("Status") == "SUCCEEDED" else 0
    return min(100, int(progressed * 100 / total))


def run_file(run_id: str) -> Path:
    return RUN_STORE / f"{run_id}.json"


def save_run(run_id: str, context: dict[str, Any], records: list[dict[str, Any]]) -> None:
    payload = {"run_id": run_id, "updated_at": utc_now(), "context": context, "records": records}
    target = run_file(run_id)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(target)


def load_run(run_id: str) -> dict[str, Any]:
    return json.loads(run_file(run_id).read_text(encoding="utf-8"))


def list_runs() -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for path in RUN_STORE.glob("*.json"):
        try:
            runs.append(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            continue
    return sorted(runs, key=lambda item: item.get("updated_at", ""), reverse=True)


def run_label(run: dict[str, Any]) -> str:
    context = run.get("context", {})
    return f"{run.get('updated_at', '')} | {context.get('snapshot_identifier', '')} | {run.get('run_id', '')[:8]}"


def update_record_from_status(record: dict[str, Any], status: dict[str, Any]) -> None:
    record["status"] = status.get("Status", "UNKNOWN")
    record["progress"] = progress_percent(status)
    record["message"] = status.get("Message", "")
    record["updated_at"] = utc_now()


def describe_restore_status(client: Any, cluster_identifier: str, request_id: str) -> dict[str, Any]:
    response = client.describe_table_restore_status(
        ClusterIdentifier=cluster_identifier,
        TableRestoreRequestId=request_id,
    )
    details = response.get("TableRestoreStatusDetails", [])
    if not details:
        raise RuntimeError(f"복구 요청 상태를 찾을 수 없습니다: {request_id}")
    return details[0]


def wait_for_restore(
    client: Any,
    cluster_identifier: str,
    request_id: str,
    poll_seconds: int,
    timeout_minutes: int,
    on_update: Any,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_minutes * 60
    not_found_deadline = time.monotonic() + 60
    while True:
        if time.monotonic() >= deadline:
            status = {"Status": "WAIT_TIMEOUT", "Message": f"{timeout_minutes}분 경과. 요청 ID로 상태 재조회가 필요합니다."}
            on_update(status)
            return status
        try:
            status = describe_restore_status(client, cluster_identifier, request_id)
        except Exception as error:
            if "TableRestoreNotFound" in str(error) and time.monotonic() < not_found_deadline:
                time.sleep(poll_seconds)
                continue
            raise
        on_update(status)
        if status.get("Status") in TERMINAL_STATES:
            return status
        time.sleep(poll_seconds)


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def data_value(field: dict[str, Any]) -> Any:
    if field.get("isNull"):
        return None
    for key in ("stringValue", "longValue", "doubleValue", "booleanValue"):
        if key in field:
            return field[key]
    return None


def wait_for_data_statement(data_client: Any, statement_id: str, poll_seconds: int, timeout_minutes: int) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_minutes * 60
    while True:
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Data API 실행 시간이 {timeout_minutes}분을 초과했습니다: {statement_id}")
        status = data_client.describe_statement(Id=statement_id)
        state = str(status.get("Status", ""))
        if state == "FINISHED":
            return status
        if state in {"FAILED", "ABORTED"}:
            raise RuntimeError(status.get("Error") or f"Data API 실행 실패: {statement_id}")
        time.sleep(poll_seconds)


def execute_data_statement(
    data_client: Any,
    cluster_identifier: str,
    database: str,
    database_user: str,
    sql: str,
    poll_seconds: int,
    timeout_minutes: int,
) -> tuple[str, dict[str, Any]]:
    if not database_user.strip():
        raise RuntimeError("원본 DROP 후 복구에는 TOML의 data_api_db_user 설정이 필요합니다.")
    response = data_client.execute_statement(
        ClusterIdentifier=cluster_identifier,
        Database=database,
        DbUser=database_user,
        Sql=sql,
    )
    statement_id = str(response["Id"])
    return statement_id, wait_for_data_statement(data_client, statement_id, poll_seconds, timeout_minutes)


def query_data_api(
    data_client: Any,
    cluster_identifier: str,
    database: str,
    database_user: str,
    sql: str,
    poll_seconds: int,
    timeout_minutes: int,
) -> list[dict[str, Any]]:
    statement_id, status = execute_data_statement(
        data_client,
        cluster_identifier,
        database,
        database_user,
        sql,
        poll_seconds,
        timeout_minutes,
    )
    if not status.get("HasResultSet"):
        return []
    rows: list[dict[str, Any]] = []
    next_token: str | None = None
    while True:
        request: dict[str, str] = {"Id": statement_id}
        if next_token:
            request["NextToken"] = next_token
        response = data_client.get_statement_result(**request)
        columns = [str(column.get("label", "")) for column in response.get("ColumnMetadata", [])]
        for record in response.get("Records", []):
            rows.append({columns[index]: data_value(value) for index, value in enumerate(record)})
        next_token = response.get("NextToken")
        if not next_token:
            return rows


def relation_exists(
    data_client: Any,
    context: dict[str, Any],
    schema_name: str,
    table_name: str,
) -> bool:
    rows = query_data_api(
        data_client,
        context["cluster_identifier"],
        context["target_database"],
        context["data_api_db_user"],
        "SELECT COUNT(*) AS relation_count "
        "FROM pg_class relation "
        "JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace "
        f"WHERE namespace.nspname = {quote_literal(schema_name)} "
        f"AND relation.relname = {quote_literal(table_name)} "
        "AND relation.relkind IN ('r', 'v', 'm')",
        context["poll_seconds"],
        context["timeout_minutes"],
    )
    return bool(rows and int(rows[0].get("relation_count") or 0))


def dependent_object_rows(
    data_client: Any,
    context: dict[str, Any],
    schema_name: str,
    table_name: str,
) -> list[dict[str, Any]]:
    sql = f"""
WITH RECURSIVE dependent_tree(object_oid, object_schema, object_name, object_kind, depth, path) AS (
    SELECT DISTINCT dependent.oid, dependent_namespace.nspname, dependent.relname, dependent.relkind, 1,
           '|' || dependent.oid::varchar || '|'
    FROM pg_depend dependency
    JOIN pg_rewrite rewrite ON rewrite.oid = dependency.objid
    JOIN pg_class dependent ON dependent.oid = rewrite.ev_class
    JOIN pg_namespace dependent_namespace ON dependent_namespace.oid = dependent.relnamespace
    JOIN pg_class source_relation ON source_relation.oid = dependency.refobjid
    JOIN pg_namespace source_namespace ON source_namespace.oid = source_relation.relnamespace
    WHERE source_namespace.nspname = {quote_literal(schema_name)}
      AND source_relation.relname = {quote_literal(table_name)}
      AND dependent.relkind IN ('v', 'm')
      AND dependent.oid <> source_relation.oid
    UNION ALL
    SELECT DISTINCT dependent.oid, dependent_namespace.nspname, dependent.relname, dependent.relkind,
           parent.depth + 1, parent.path || dependent.oid::varchar || '|'
    FROM dependent_tree parent
    JOIN pg_depend dependency ON dependency.refobjid = parent.object_oid
    JOIN pg_rewrite rewrite ON rewrite.oid = dependency.objid
    JOIN pg_class dependent ON dependent.oid = rewrite.ev_class
    JOIN pg_namespace dependent_namespace ON dependent_namespace.oid = dependent.relnamespace
    WHERE dependent.relkind IN ('v', 'm')
      AND POSITION('|' || dependent.oid::varchar || '|' IN parent.path) = 0
)
SELECT object_schema, object_name, object_kind, MAX(depth) AS dependency_depth
FROM dependent_tree
GROUP BY object_schema, object_name, object_kind
ORDER BY dependency_depth, object_schema, object_name
"""
    return query_data_api(
        data_client,
        context["cluster_identifier"],
        context["target_database"],
        context["data_api_db_user"],
        sql,
        context["poll_seconds"],
        context["timeout_minutes"],
    )


def object_definition(
    data_client: Any,
    context: dict[str, Any],
    schema_name: str,
    object_name: str,
) -> str:
    rows = query_data_api(
        data_client,
        context["cluster_identifier"],
        context["target_database"],
        context["data_api_db_user"],
        f"SHOW VIEW {quote_identifier(schema_name)}.{quote_identifier(object_name)}",
        context["poll_seconds"],
        context["timeout_minutes"],
    )
    if len(rows) != 1 or not rows[0]:
        raise RuntimeError(f"참조 오브젝트 정의를 읽을 수 없습니다: {schema_name}.{object_name}")
    definition = str(next(iter(rows[0].values())) or "").strip().rstrip(";")
    if not definition:
        raise RuntimeError(f"참조 오브젝트 정의가 비어 있습니다: {schema_name}.{object_name}")
    return definition


def object_owner_and_grants(
    data_client: Any,
    context: dict[str, Any],
    schema_name: str,
    object_name: str,
) -> tuple[str, list[dict[str, Any]]]:
    owner_rows = query_data_api(
        data_client,
        context["cluster_identifier"],
        context["target_database"],
        context["data_api_db_user"],
        "SELECT user_info.usename AS owner_name "
        "FROM pg_class relation "
        "JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace "
        "JOIN pg_user user_info ON user_info.usesysid = relation.relowner "
        f"WHERE namespace.nspname = {quote_literal(schema_name)} "
        f"AND relation.relname = {quote_literal(object_name)}",
        context["poll_seconds"],
        context["timeout_minutes"],
    )
    owner_name = str(owner_rows[0].get("owner_name") or "") if owner_rows else ""
    grants = query_data_api(
        data_client,
        context["cluster_identifier"],
        context["target_database"],
        context["data_api_db_user"],
        "SELECT privilege_type, identity_name, identity_type, admin_option "
        "FROM svv_relation_privileges "
        f"WHERE namespace_name = {quote_literal(schema_name)} "
        f"AND relation_name = {quote_literal(object_name)}",
        context["poll_seconds"],
        context["timeout_minutes"],
    )
    return owner_name, grants


def rls_attachments(
    data_client: Any,
    context: dict[str, Any],
    schema_name: str,
    object_name: str,
) -> list[dict[str, Any]]:
    rows = query_data_api(
        data_client,
        context["cluster_identifier"],
        context["target_database"],
        context["data_api_db_user"],
        f"SHOW RLS POLICIES ON {quote_identifier(schema_name)}.{quote_identifier(object_name)}",
        context["poll_seconds"],
        context["timeout_minutes"],
    )
    return [
        {
            "policy_name": row.get("policy_name"),
            "identity_name": row.get("grantee") or row.get("identity_name"),
            "identity_type": row.get("grantee_kind") or row.get("identity_type"),
        }
        for row in rows
    ]


def capture_recovery_artifacts(data_client: Any, context: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    schema_name = record["target_schema"]
    table_name = record["target_table"]
    if not relation_exists(data_client, context, schema_name, table_name):
        return {"table": None, "dependent_objects": []}
    table_owner, table_grants = object_owner_and_grants(data_client, context, schema_name, table_name)
    table_artifact = {
        "schema_name": schema_name,
        "object_name": table_name,
        "object_kind": "r",
        "owner_name": table_owner,
        "grants": table_grants,
        "rls_attachments": rls_attachments(data_client, context, schema_name, table_name),
    }
    dependent_objects: list[dict[str, Any]] = []
    for item in dependent_object_rows(data_client, context, schema_name, table_name):
        object_schema = str(item["object_schema"])
        object_name = str(item["object_name"])
        owner_name, grants = object_owner_and_grants(data_client, context, object_schema, object_name)
        dependent_objects.append(
            {
                "schema_name": object_schema,
                "object_name": object_name,
                "object_kind": str(item["object_kind"]),
                "dependency_depth": int(item.get("dependency_depth") or 0),
                "definition": object_definition(data_client, context, object_schema, object_name),
                "owner_name": owner_name,
                "grants": grants,
                "rls_attachments": rls_attachments(data_client, context, object_schema, object_name),
            }
        )
    return {"table": table_artifact, "dependent_objects": dependent_objects}


def drop_target_table(
    data_client: Any,
    cluster_identifier: str,
    database: str,
    database_user: str,
    schema_name: str,
    table_name: str,
    poll_seconds: int,
    timeout_minutes: int,
    cascade: bool,
) -> str:
    statement_id, _ = execute_data_statement(
        data_client,
        cluster_identifier,
        database,
        database_user,
        f"DROP TABLE IF EXISTS {quote_identifier(schema_name)}.{quote_identifier(table_name)}" + (" CASCADE" if cascade else ""),
        poll_seconds,
        timeout_minutes,
    )
    return statement_id


def grantee_sql(grant: dict[str, Any]) -> str:
    identity_type = str(grant.get("identity_type") or "").lower()
    identity_name = str(grant.get("identity_name") or "")
    if identity_type == "public":
        return "PUBLIC"
    if identity_type == "role":
        return f"ROLE {quote_identifier(identity_name)}"
    if identity_type == "group":
        return f"GROUP {quote_identifier(identity_name)}"
    return quote_identifier(identity_name)


def execute_recovery_sql(data_client: Any, context: dict[str, Any], sql: str) -> None:
    execute_data_statement(
        data_client,
        context["cluster_identifier"],
        context["target_database"],
        context["data_api_db_user"],
        sql,
        context["poll_seconds"],
        context["timeout_minutes"],
    )


def restore_owner_and_grants(data_client: Any, context: dict[str, Any], artifact: dict[str, Any]) -> None:
    relation = f"{quote_identifier(artifact['schema_name'])}.{quote_identifier(artifact['object_name'])}"
    object_kind = artifact["object_kind"]
    if artifact.get("owner_name"):
        alter_command = "ALTER MATERIALIZED VIEW" if object_kind == "m" else "ALTER TABLE"
        execute_recovery_sql(data_client, context, f"{alter_command} {relation} OWNER TO {quote_identifier(str(artifact['owner_name']))}")
    valid_privileges = {"SELECT", "INSERT", "UPDATE", "DELETE", "REFERENCES", "ALTER", "DROP", "TRUNCATE"}
    for grant in artifact.get("grants", []):
        privilege = str(grant.get("privilege_type") or "").upper()
        if privilege not in valid_privileges:
            continue
        grant_option = " WITH GRANT OPTION" if grant.get("admin_option") and str(grant.get("identity_type") or "").lower() not in {"group", "public"} else ""
        execute_recovery_sql(
            data_client,
            context,
            f"GRANT {privilege} ON TABLE {relation} TO {grantee_sql(grant)}{grant_option}",
        )


def restore_rls_attachments(data_client: Any, context: dict[str, Any], artifact: dict[str, Any]) -> None:
    relation = f"{quote_identifier(artifact['schema_name'])}.{quote_identifier(artifact['object_name'])}"
    attachments = artifact.get("rls_attachments", [])
    if not attachments:
        return
    object_kind = artifact["object_kind"]
    alter_command = "ALTER MATERIALIZED VIEW" if object_kind == "m" else "ALTER TABLE"
    execute_recovery_sql(data_client, context, f"{alter_command} {relation} ROW LEVEL SECURITY ON")
    for attachment in attachments:
        policy_name = str(attachment.get("policy_name") or "")
        if not policy_name:
            raise RuntimeError(f"RLS 정책 이름을 읽을 수 없습니다: {relation}")
        execute_recovery_sql(
            data_client,
            context,
            f"ATTACH RLS POLICY {quote_identifier(policy_name)} ON TABLE {relation} TO {grantee_sql(attachment)}",
        )


def recreate_dependent_objects(data_client: Any, context: dict[str, Any], artifacts: dict[str, Any]) -> None:
    table_artifact = artifacts.get("table")
    if table_artifact:
        restore_owner_and_grants(data_client, context, table_artifact)
        restore_rls_attachments(data_client, context, table_artifact)
    for artifact in artifacts.get("dependent_objects", []):
        relation = f"{quote_identifier(artifact['schema_name'])}.{quote_identifier(artifact['object_name'])}"
        definition = str(artifact["definition"]).strip().rstrip(";")
        if definition.lower().startswith("create "):
            create_sql = definition
        elif artifact["object_kind"] == "m":
            create_sql = f"CREATE MATERIALIZED VIEW {relation} AS {definition}"
        else:
            create_sql = f"CREATE VIEW {relation} AS {definition}"
        execute_recovery_sql(data_client, context, create_sql)
        restore_owner_and_grants(data_client, context, artifact)
        restore_rls_attachments(data_client, context, artifact)


def save_and_render(run_id: str, context: dict[str, Any], records: list[dict[str, Any]], summary_placeholder: Any, records_placeholder: Any) -> None:
    st.session_state.restore_records = records
    st.session_state.active_run_id = run_id
    st.session_state.active_run_context = context
    save_run(run_id, context, records)
    display_restore_summary(summary_placeholder, records)
    display_restore_records(records_placeholder, records)


def execute_queued_records(
    client: Any,
    data_client: Any | None,
    run_id: str,
    context: dict[str, Any],
    records: list[dict[str, Any]],
    continue_on_failure: bool,
    summary_placeholder: Any,
    records_placeholder: Any,
) -> None:
    queued = [record for record in records if record["status"] == "QUEUED"]
    if not queued:
        st.info("실행할 대기 테이블이 없습니다.", icon=":material/info:")
        return
    total = len(records)
    progress = st.progress(0, text="복구 실행을 준비하고 있습니다.")
    with st.status("테이블 복구를 순차 실행하고 있습니다.", expanded=True) as status_panel:
        for record in queued:
            source = f"{record['source_schema']}.{record['source_table']}"
            target = f"{record['target_schema']}.{record['target_table']}"
            status_panel.write(f"{record['sequence']}/{total} {source} → {target} 복구 요청")
            try:
                if context["restore_mode"] == "replace":
                    if data_client is None:
                        raise RuntimeError("원본 DROP 후 복구에 사용할 Redshift Data API 클라이언트를 만들 수 없습니다.")
                    if not record.get("recovery_artifacts"):
                        record["recovery_artifacts"] = capture_recovery_artifacts(data_client, context, record)
                    artifact_count = len(record["recovery_artifacts"]["dependent_objects"])
                    record["message"] = f"참조 오브젝트 {artifact_count}건과 권한 정보를 저장했습니다."
                    record["updated_at"] = utc_now()
                    save_and_render(run_id, context, records, summary_placeholder, records_placeholder)
                    status_panel.write(f"{record['sequence']}/{total} {target} 기존 테이블 삭제")
                    drop_statement_id = drop_target_table(
                        data_client=data_client,
                        cluster_identifier=context["cluster_identifier"],
                        database=context["target_database"],
                        database_user=context["data_api_db_user"],
                        schema_name=record["target_schema"],
                        table_name=record["target_table"],
                        poll_seconds=context["poll_seconds"],
                        timeout_minutes=context["timeout_minutes"],
                        cascade=True,
                    )
                    record["message"] = f"기존 테이블 삭제 완료: {drop_statement_id}"
                    record["updated_at"] = utc_now()
                    save_and_render(run_id, context, records, summary_placeholder, records_placeholder)
                response = client.restore_table_from_cluster_snapshot(
                    ClusterIdentifier=context["cluster_identifier"],
                    SnapshotIdentifier=context["snapshot_identifier"],
                    SourceDatabaseName=context["source_database"],
                    SourceSchemaName=record["source_schema"],
                    SourceTableName=record["source_table"],
                    TargetDatabaseName=context["target_database"],
                    TargetSchemaName=record["target_schema"],
                    NewTableName=record["target_table"],
                    EnableCaseSensitiveIdentifier=context["case_sensitive_identifiers"],
                )
                initial_status = response["TableRestoreStatus"]
                record["request_id"] = initial_status["TableRestoreRequestId"]

                def on_update(current_status: dict[str, Any]) -> None:
                    update_record_from_status(record, current_status)
                    save_and_render(run_id, context, records, summary_placeholder, records_placeholder)
                    completed = sum(item["status"] in TERMINAL_STATES for item in records)
                    progress.progress(
                        min(1.0, (completed + record["progress"] / 100) / total),
                        text=f"{record['sequence']}/{total} {target}: {record['status']}",
                    )

                final_status = wait_for_restore(
                    client=client,
                    cluster_identifier=context["cluster_identifier"],
                    request_id=record["request_id"],
                    poll_seconds=context["poll_seconds"],
                    timeout_minutes=context["timeout_minutes"],
                    on_update=on_update,
                )
                if final_status.get("Status") == "SUCCEEDED" and context["restore_mode"] == "replace":
                    if data_client is None:
                        raise RuntimeError("참조 오브젝트 재생성에 사용할 Redshift Data API 클라이언트를 만들 수 없습니다.")
                    recreate_dependent_objects(data_client, context, record.get("recovery_artifacts", {}))
                    record["message"] = "복구와 참조 오브젝트 재생성을 완료했습니다."
                    record["updated_at"] = utc_now()
                    save_and_render(run_id, context, records, summary_placeholder, records_placeholder)
                status_panel.write(f"{record['sequence']}/{total} {target}: {final_status['Status']}")
            except Exception as error:
                record["status"] = "FAILED"
                record["message"] = str(error)
                record["updated_at"] = utc_now()
                save_and_render(run_id, context, records, summary_placeholder, records_placeholder)
                status_panel.write(f"{record['sequence']}/{total} {target}: FAILED")
            if record["status"] == "WAIT_TIMEOUT":
                status_panel.update(label="상태 확인 시간이 초과되어 중단했습니다. 실행 이력에서 상태를 재조회해 주십시오.", state="error", expanded=True)
                return
            if record["status"] in {"FAILED", "CANCELED"} and not continue_on_failure:
                status_panel.update(label="실패로 인해 후속 복구를 중단했습니다.", state="error", expanded=True)
                return
        failed_count = sum(record["status"] in {"FAILED", "CANCELED"} for record in records)
        if failed_count:
            status_panel.update(label="복구가 완료되었으나 실패 또는 취소된 테이블이 있습니다.", state="error", expanded=True)
        else:
            status_panel.update(label="모든 테이블 복구가 완료되었습니다.", state="complete", expanded=True)


def refresh_run_statuses(client: Any, run: dict[str, Any]) -> dict[str, Any]:
    context = run["context"]
    for record in run["records"]:
        if not record.get("request_id"):
            continue
        try:
            update_record_from_status(record, describe_restore_status(client, context["cluster_identifier"], record["request_id"]))
        except Exception as error:
            record["message"] = f"상태 재조회 실패: {error}"
            record["updated_at"] = utc_now()
    if context.get("job_state") not in {"STARTING", "RUNNING"}:
        save_run(run["run_id"], context, run["records"])
    return run


def retry_dependent_object_recreation(data_client: Any, client: Any, run: dict[str, Any]) -> dict[str, Any]:
    context = run["context"]
    for record in run["records"]:
        if not record.get("request_id") or not record.get("recovery_artifacts"):
            continue
        try:
            status = describe_restore_status(client, context["cluster_identifier"], record["request_id"])
            update_record_from_status(record, status)
            if record["status"] != "SUCCEEDED":
                continue
            recreate_dependent_objects(data_client, context, record["recovery_artifacts"])
            record["message"] = "참조 오브젝트 재생성을 완료했습니다."
            record["updated_at"] = utc_now()
        except Exception as error:
            record["status"] = "FAILED"
            record["message"] = f"참조 오브젝트 재생성 실패: {error}"
            record["updated_at"] = utc_now()
    save_run(run["run_id"], context, run["records"])
    return run


def save_worker_state(run_id: str, context: dict[str, Any], records: list[dict[str, Any]]) -> None:
    context["job_updated_at"] = utc_now()
    save_run(run_id, context, records)


def execute_restore_worker(run_id: str, settings: dict[str, str]) -> None:
    run = load_run(run_id)
    context = run["context"]
    records = run["records"]
    context["job_state"] = "RUNNING"
    context["job_started_at"] = context.get("job_started_at") or utc_now()
    save_worker_state(run_id, context, records)
    try:
        client = get_redshift_client(settings)
        data_client = get_redshift_data_client(settings) if context["restore_mode"] == "replace" else None
        total = len(records)
        for record in records:
            if record["status"] != "QUEUED":
                continue
            source = f"{record['source_schema']}.{record['source_table']}"
            target = f"{record['target_schema']}.{record['target_table']}"
            try:
                record["status"] = "IN_PROGRESS"
                record["started_at"] = record.get("started_at") or utc_now()
                record["message"] = f"{record['sequence']}/{total} {source} → {target} 복구 준비"
                record["updated_at"] = utc_now()
                save_worker_state(run_id, context, records)
                if context["restore_mode"] == "replace":
                    if data_client is None:
                        raise RuntimeError("원본 DROP 후 복구에 필요한 Redshift Data API 클라이언트를 만들 수 없습니다.")
                    if not record.get("recovery_artifacts"):
                        raise RuntimeError("참조 오브젝트 확보 정보가 없습니다. 새 복구 작업을 시작해 주십시오.")
                    drop_statement_id = drop_target_table(
                        data_client=data_client,
                        cluster_identifier=context["cluster_identifier"],
                        database=context["target_database"],
                        database_user=context["data_api_db_user"],
                        schema_name=record["target_schema"],
                        table_name=record["target_table"],
                        poll_seconds=context["poll_seconds"],
                        timeout_minutes=context["timeout_minutes"],
                        cascade=True,
                    )
                    record["message"] = f"기존 테이블 삭제 완료: {drop_statement_id}"
                    record["updated_at"] = utc_now()
                    save_worker_state(run_id, context, records)
                response = client.restore_table_from_cluster_snapshot(
                    ClusterIdentifier=context["cluster_identifier"],
                    SnapshotIdentifier=context["snapshot_identifier"],
                    SourceDatabaseName=context["source_database"],
                    SourceSchemaName=record["source_schema"],
                    SourceTableName=record["source_table"],
                    TargetDatabaseName=context["target_database"],
                    TargetSchemaName=record["target_schema"],
                    NewTableName=record["target_table"],
                    EnableCaseSensitiveIdentifier=context["case_sensitive_identifiers"],
                )
                initial_status = response["TableRestoreStatus"]
                record["request_id"] = initial_status["TableRestoreRequestId"]
                update_record_from_status(record, initial_status)
                save_worker_state(run_id, context, records)

                def on_update(current_status: dict[str, Any]) -> None:
                    update_record_from_status(record, current_status)
                    save_worker_state(run_id, context, records)

                final_status = wait_for_restore(
                    client=client,
                    cluster_identifier=context["cluster_identifier"],
                    request_id=record["request_id"],
                    poll_seconds=context["poll_seconds"],
                    timeout_minutes=context["timeout_minutes"],
                    on_update=on_update,
                )
                if final_status.get("Status") == "WAIT_TIMEOUT":
                    context["job_state"] = "WAITING"
                    save_worker_state(run_id, context, records)
                    return
                if final_status.get("Status") == "SUCCEEDED" and context["restore_mode"] == "replace":
                    if data_client is None:
                        raise RuntimeError("참조 오브젝트 재생성에 필요한 Redshift Data API 클라이언트를 만들 수 없습니다.")
                    record["message"] = "참조 오브젝트 재생성 중"
                    record["updated_at"] = utc_now()
                    save_worker_state(run_id, context, records)
                    recreate_dependent_objects(data_client, context, record["recovery_artifacts"])
                    record["message"] = "복구 및 참조 오브젝트 재생성 완료"
                    record["updated_at"] = utc_now()
                    save_worker_state(run_id, context, records)
                record["finished_at"] = utc_now()
                record["updated_at"] = utc_now()
                save_worker_state(run_id, context, records)
            except Exception as error:
                record["status"] = "FAILED"
                record["message"] = str(error)
                record["finished_at"] = utc_now()
                record["updated_at"] = utc_now()
                save_worker_state(run_id, context, records)
                if not context.get("continue_on_failure", True):
                    context["job_state"] = "STOPPED"
                    save_worker_state(run_id, context, records)
                    return
        context["job_state"] = "COMPLETED"
        context["job_completed_at"] = utc_now()
        save_worker_state(run_id, context, records)
    except Exception as error:
        context["job_state"] = "FAILED"
        context["job_error"] = str(error)
        save_worker_state(run_id, context, records)


def start_restore_worker(run_id: str) -> int:
    command = [sys.executable, str(Path(__file__).resolve()), "--restore-worker", run_id]
    process = subprocess.Popen(
        command,
        cwd=str(PROJECT_ROOT),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    return int(process.pid)


def render_live_restore_state() -> None:
    run_id = st.session_state.active_run_id
    if run_id:
        try:
            run = load_run(run_id)
            st.session_state.restore_records = run["records"]
            st.session_state.active_run_context = run["context"]
        except (OSError, json.JSONDecodeError):
            pass
    if not st.session_state.restore_records:
        return
    display_restore_summary(st, st.session_state.restore_records)
    display_restore_records(st, st.session_state.restore_records)


if __name__ == "__main__" and len(sys.argv) == 3 and sys.argv[1] == "--restore-worker":
    saved_run = load_run(sys.argv[2])
    execute_restore_worker(sys.argv[2], get_redshift_settings(text(saved_run["context"].get("sec_sect_nm"))))
    raise SystemExit


initialize_state()

runtime = runtime_context()
database_options = get_database_options(runtime)
if not database_options:
    st.error("접속정보에 사용 중인 대상 Redshift DB 접속을 등록하십시오.", icon=":material/error:")
    st.stop()

with st.container(border=True):
    selected_database = st.selectbox(
        "복구 대상 DB",
        database_options,
        format_func=lambda item: item["label"],
        key="selected_database",
    )
    settings = selected_database["settings"]
    poll_seconds = setting_int(settings, "poll_seconds", 10, 5, 120)
    timeout_minutes = setting_int(settings, "table_timeout_minutes", 360, 5, 1440)
    continue_on_failure = True
    cluster_identifier = selected_database["cluster_identifier"]
    source_database = selected_database["source_database"]
    target_database = selected_database["target_database"]
    default_schema = selected_database["default_schema"]
    data_api_db_user = selected_database["data_api_db_user"]
    case_sensitive = setting_bool(settings, "case_sensitive_identifiers", False)
    target_access_key = "|".join(
        [selected_database["label"], cluster_identifier, source_database, target_database]
    )
    if target_access_key not in st.session_state.target_access_checks:
        try:
            validate_target_access(settings, cluster_identifier)
            st.session_state.target_access_checks[target_access_key] = ""
        except Exception as error:
            st.session_state.target_access_checks[target_access_key] = str(error)
    target_access_error = st.session_state.target_access_checks[target_access_key]
    if target_access_error:
        st.error(f"{selected_database['label']} 접근 확인 실패: {target_access_error}", icon=":material/error:")

st.subheader(":material/restore_page: Redshift 스냅샷 복구")

with st.container(border=True):
    st.caption("1. 스냅샷 선택")
    snapshot_type_label = st.segmented_control(
        "스냅샷 유형",
        options=list(SNAPSHOT_TYPES),
        default="전체",
        required=True,
        width="content",
    )
    snapshot_context = json.dumps(
        {"cluster_identifier": cluster_identifier, "snapshot_type": snapshot_type_label},
        ensure_ascii=False,
        sort_keys=True,
    )
    refresh_snapshots = st.button("스냅샷 조회", icon=":material/refresh:")
    if refresh_snapshots:
        if not cluster_identifier.strip():
            st.error("선택한 DB의 클러스터 식별자가 TOML에 설정되지 않았습니다.", icon=":material/error:")
        else:
            try:
                with st.spinner("available 상태의 스냅샷을 조회하고 있습니다."):
                    st.session_state.snapshots = list_snapshots(
                        get_redshift_client(settings),
                        cluster_identifier.strip(),
                        SNAPSHOT_TYPES[snapshot_type_label],
                    )
                st.session_state.snapshot_context = snapshot_context
                st.success(f"사용 가능한 {snapshot_type_label} 스냅샷 {len(st.session_state.snapshots)}건을 조회했습니다.", icon=":material/check_circle:")
            except Exception as error:
                st.error(f"스냅샷 조회 실패: {error}", icon=":material/error:")
    snapshot_identifier = ""
    if st.session_state.snapshots and st.session_state.snapshot_context == snapshot_context:
        selected_snapshot = st.selectbox("조회된 available 스냅샷", st.session_state.snapshots, format_func=snapshot_label)
        snapshot_identifier = str(selected_snapshot.get("SnapshotIdentifier", ""))

with st.container(border=True):
    st.caption("2. 복구 테이블 입력")
    restore_mode_label = st.segmented_control(
        "복구 방식",
        options=list(RESTORE_MODES),
        default="별도 테이블 복구",
        required=True,
        width="content",
    )
    restore_mode = RESTORE_MODES[restore_mode_label]
    edited_tables = st.data_editor(
        normalize_table_frame(st.session_state.restore_table_draft, default_schema.strip()),
        key="table_editor",
        num_rows="dynamic",
        hide_index=True,
        column_config={
            "source_schema": st.column_config.TextColumn("원본 스키마", width="small", required=True),
            "source_table": st.column_config.TextColumn("원본 테이블", width="medium", required=True),
        },
    )

tables_to_restore, validation_errors = valid_restore_tables(normalize_table_frame(edited_tables, default_schema.strip()), case_sensitive)
drop_preflight_key = ""
drop_preflight_ready = True
if restore_mode == "replace":
    drop_preflight_key = json.dumps(
        {
            "database": selected_database["label"],
            "cluster": cluster_identifier,
            "target_database": target_database,
            "database_user": data_api_db_user,
            "tables": tables_to_restore.to_dict(orient="records"),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    drop_preflight_ready = st.session_state.drop_preflight.get("key") == drop_preflight_key
with st.container(border=True):
    st.caption("3. 실행 전 검증 및 복구")
    if validation_errors:
        for message in validation_errors:
            st.warning(message, icon=":material/warning:")
    if restore_mode == "replace":
        can_run_preflight = not validation_errors and bool(
            cluster_identifier.strip() and target_database.strip() and data_api_db_user.strip() and not target_access_error
        )
        inspect_dependencies = st.button(
            "참조 오브젝트 확인",
            icon=":material/account_tree:",
            disabled=not can_run_preflight,
        )
        if inspect_dependencies:
            try:
                preflight_context = {
                    "cluster_identifier": cluster_identifier.strip(),
                    "target_database": target_database.strip(),
                    "data_api_db_user": data_api_db_user.strip(),
                    "poll_seconds": int(poll_seconds),
                    "timeout_minutes": int(timeout_minutes),
                }
                preflight_records = build_records(tables_to_restore, "replace", "")
                data_client = get_redshift_data_client(settings)
                artifacts = [capture_recovery_artifacts(data_client, preflight_context, record) for record in preflight_records]
                st.session_state.drop_preflight = {"key": drop_preflight_key, "artifacts": artifacts}
                drop_preflight_ready = True
            except Exception as error:
                st.session_state.drop_preflight = {}
                drop_preflight_ready = False
                st.error(f"참조 오브젝트 확인 실패: {error}", icon=":material/error:")
        if drop_preflight_ready:
            preview_rows = []
            for source_row, artifacts in zip(tables_to_restore.to_dict(orient="records"), st.session_state.drop_preflight.get("artifacts", [])):
                preview_rows.append(
                    {
                        "원본": f"{source_row['source_schema']}.{source_row['source_table']}",
                        "참조 오브젝트": len(artifacts.get("dependent_objects", [])),
                    }
                )
            st.dataframe(pd.DataFrame(preview_rows), hide_index=True)
    confirmation_label = (
        "기존 대상 테이블을 삭제하고 동일한 이름으로 복구합니다."
        if restore_mode == "replace"
        else "원본 테이블명에 실행 시각을 붙인 별도 테이블로 복구합니다."
    )
    confirmed = st.checkbox(confirmation_label)
    start_restore = st.button(
        "복구 시작",
        type="primary",
        icon=":material/play_arrow:",
        disabled=not confirmed or not drop_preflight_ready,
    )
    if start_restore:
        execution_errors = validation_errors.copy()
        if not cluster_identifier.strip():
            execution_errors.append("선택한 DB의 클러스터 식별자가 TOML에 설정되지 않았습니다.")
        if target_access_error:
            execution_errors.append(f"{selected_database['label']} 접근 확인 실패: {target_access_error}")
        if not snapshot_identifier.strip():
            execution_errors.append("스냅샷 식별자를 선택하거나 입력해 주십시오.")
        if not source_database.strip() or not target_database.strip():
            execution_errors.append("원본 및 대상 데이터베이스를 입력해 주십시오.")
        if restore_mode == "replace" and not data_api_db_user.strip():
            execution_errors.append("원본 DROP 후 복구에는 TOML의 data_api_db_user 설정이 필요합니다.")
        if restore_mode == "replace" and not drop_preflight_ready:
            execution_errors.append("참조 오브젝트 확인을 먼저 실행해 주십시오.")
        if execution_errors:
            for message in execution_errors:
                st.error(message, icon=":material/error:")
        else:
            try:
                client = get_redshift_client(settings)
                validate_snapshot(client, cluster_identifier.strip(), snapshot_identifier.strip())
                run_id = f"restore-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
                context = {
                    "conn_id": selected_database["conn_id"],
                    "sec_sect_nm": selected_database["sec_sect_nm"],
                    "cluster_identifier": cluster_identifier.strip(),
                    "snapshot_identifier": snapshot_identifier.strip(),
                    "source_database": source_database.strip(),
                    "target_database": target_database.strip(),
                    "data_api_db_user": data_api_db_user.strip(),
                    "restore_mode": restore_mode,
                    "case_sensitive_identifiers": case_sensitive,
                    "poll_seconds": int(poll_seconds),
                    "timeout_minutes": int(timeout_minutes),
                    "continue_on_failure": continue_on_failure,
                    "job_state": "STARTING",
                }
                restore_suffix = datetime.now().strftime("%Y%m%d%H%M%S")
                records = build_records(tables_to_restore, restore_mode, restore_suffix)
                if restore_mode == "replace":
                    for record, artifacts in zip(records, st.session_state.drop_preflight.get("artifacts", [])):
                        record["recovery_artifacts"] = artifacts
                save_run(run_id, context, records)
                try:
                    context["worker_pid"] = start_restore_worker(run_id)
                except Exception as error:
                    context["job_state"] = "FAILED"
                    context["job_error"] = str(error)
                    save_run(run_id, context, records)
                    raise
                save_run(run_id, context, records)
                st.session_state.restore_records = records
                st.session_state.active_run_id = run_id
                st.session_state.active_run_context = context
                st.rerun()
            except Exception as error:
                st.error(f"복구 실행 전 검증 실패: {error}", icon=":material/error:")


@st.fragment(run_every="5s")
def live_restore_fragment() -> None:
    render_live_restore_state()


live_restore_fragment()


with st.expander("실행 이력 및 상태 재조회", icon=":material/history:"):
    saved_runs = list_runs()
    if not saved_runs:
        st.caption("저장된 실행 이력이 없습니다.")
    else:
        selected_run = st.selectbox("저장된 실행", saved_runs, format_func=run_label)
        history_actions = st.container(horizontal=True, horizontal_alignment="left")
        with history_actions:
            refresh_selected_run = st.button("상태 재조회", icon=":material/sync:")
            show_selected_run = st.button("이력 표시", icon=":material/visibility:")
            retry_dependencies = st.button(
                "참조 오브젝트 재생성 재시도",
                icon=":material/restart_alt:",
                disabled=selected_run.get("context", {}).get("restore_mode") != "replace",
            )
        if refresh_selected_run:
            try:
                selected_run = refresh_run_statuses(get_redshift_client(settings), selected_run)
                st.session_state.restore_records = selected_run["records"]
                st.session_state.active_run_id = selected_run["run_id"]
                st.session_state.active_run_context = selected_run["context"]
                st.rerun()
            except Exception as error:
                st.error(f"상태 재조회 실패: {error}", icon=":material/error:")
        if show_selected_run:
            st.session_state.restore_records = selected_run["records"]
            st.session_state.active_run_id = selected_run["run_id"]
            st.session_state.active_run_context = selected_run["context"]
            st.rerun()
        if retry_dependencies:
            try:
                selected_run = retry_dependent_object_recreation(
                    get_redshift_data_client(settings),
                    get_redshift_client(settings),
                    selected_run,
                )
                st.session_state.restore_records = selected_run["records"]
                st.session_state.active_run_id = selected_run["run_id"]
                st.session_state.active_run_context = selected_run["context"]
                st.rerun()
            except Exception as error:
                st.error(f"참조 오브젝트 재생성 재시도 실패: {error}", icon=":material/error:")

from __future__ import annotations

from datetime import datetime
from time import perf_counter
from typing import Any

import oracledb
import pandas as pd
import streamlit as st

try:
    import pyodbc
except ImportError:
    pyodbc = None


SOURCE_SERVERS = ("CERDB", "INSIDEBANK", "JBNDB", "MEMDB", "PREEDDB", "SALDB")
REFRESH_OPTIONS = {"1초": 1, "5초": 5, "10초": 10, "30초": 30, "1분": 60, "5분": 300, "10분": 600}
ORACLE_SESSION_USERS = ("PCERP_RENTALAPP", "PCERP_RENTALAPP_MIG")

SITUATION_BOARD_HTML = """
<div id="situation-board"></div>
"""

SITUATION_BOARD_CSS = """
#situation-board { color: #eef7ff; font-family: var(--st-font); }
.board-grid { display: grid; grid-template-columns: minmax(0, 2fr) minmax(360px, 1fr); gap: 1rem; align-items: start; }
.board-section-title { margin: 0 0 .6rem; color: #f4f8fc; font-size: 1.45rem; font-weight: 760; }
.mssql-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: .65rem; }
.server-card { min-height: 132px; box-sizing: border-box; padding: .62rem .72rem; border: 1px solid #254d73; border-radius: 8px; border-top-width: 3px; background: rgba(16, 33, 56, .9); }
.server-card-oracle { min-height: 210px; }
.server-card-ok { border-top-color: #69e393; }
.server-card-wait { border-top-color: #ffc466; }
.server-card-fail { border-top-color: #ff8d97; }
.server-card-auth { border-top-color: #ffc466; }
.server-card-setup { border-top-color: #6fc4ff; }
.server-card-top { display: flex; align-items: baseline; justify-content: space-between; gap: .5rem; }
.server-card-name { color: #d5e1ed; font-size: 1.1rem; font-weight: 800; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.server-card-state { margin-top: .28rem; font-size: .88rem; font-weight: 760; }
.server-card-state-ok { color: #69e393; }
.server-card-state-wait { color: #ffc466; }
.server-card-state-fail { color: #ff8d97; }
.server-card-state-auth { color: #ffc466; }
.server-card-state-setup { color: #6fc4ff; }
.server-card-detail { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: .3rem; margin-top: .48rem; }
.server-card-detail-label { color: #9db5cc; font-size: .62rem; }
.server-card-detail-value { color: #eef7ff; font-size: .75rem; font-weight: 650; margin-top: .05rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.server-card-server-time { flex: 0 0 auto; color: #9db5cc; font-size: .75rem; white-space: nowrap; }
.session-trend { display: flex; align-items: center; gap: .34rem; margin-top: .38rem; }
.session-trend-name { color: #9db5cc; font-size: .59rem; white-space: nowrap; }
.session-trend-value { color: #eef7ff; font-size: .65rem; font-weight: 700; min-width: 2.8rem; text-align: right; white-space: nowrap; }
.session-trend-chart { flex: 1; height: 14px; }
.session-trend-chart path { fill: none; stroke: #75c7ff; stroke-width: 1.8; stroke-linecap: round; stroke-linejoin: round; }
.server-card-error { color: #ff8d97; font-size: .62rem; line-height: 1.2; margin-top: .42rem; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }
.server-card-error-wait, .server-card-error-auth { color: #ffc466; }
.tablespace-title { margin: 1rem 0 .6rem; color: #f4f8fc; font-size: 1.45rem; font-weight: 760; }
.tablespace-table { width: 100%; border-collapse: separate; border-spacing: 0; overflow: hidden; border: 1px solid #284666; border-radius: 8px; font-size: .78rem; }
.tablespace-table th, .tablespace-table td { padding: .44rem .5rem; text-align: left; border-bottom: 1px solid rgba(157, 181, 204, .14); white-space: nowrap; }
.tablespace-table th { color: #9db5cc; background: #142b48; font-weight: 700; }
.tablespace-table td { color: #eef7ff; }
.tablespace-table tr:last-child td { border-bottom: 0; }
.empty-tablespace { color: #9db5cc; font-size: .78rem; }
@media (max-width: 980px) { .board-grid { grid-template-columns: 1fr; } }
"""

SITUATION_BOARD_JS = """
export default function(component) {
  const { data, parentElement } = component
  const root = parentElement.querySelector("#situation-board")
  if (!root) return

  const text = (value) => value === null || value === undefined || value === "" ? "-" : String(value)
  const escape = (value) => text(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#039;")
  const chart = (values) => {
    const samples = (values || []).slice(-60).map(Number).filter(Number.isFinite)
    if (!samples.length) return '<svg class="session-trend-chart" viewBox="0 0 120 14" preserveAspectRatio="none"></svg>'
    const low = Math.min(...samples)
    const high = Math.max(...samples)
    const span = Math.max(samples.length - 1, 1)
    const scale = Math.max(high - low, 1)
    const points = samples.map((value, index) => `${(index * 120 / span).toFixed(1)},${(12 - (value - low) * 10 / scale).toFixed(1)}`)
    if (points.length === 1) points.push(points[0].replace("0.0,", "120.0,"))
    return `<svg class="session-trend-chart" viewBox="0 0 120 14" preserveAspectRatio="none"><path d="M ${points.join(" L ")}" /></svg>`
  }
  const trend = (item) => `<div class="session-trend"><span class="session-trend-name">${escape(item.name)}</span><span class="session-trend-value">${escape(item.value)}</span>${chart(item.history)}</div>`
  const card = (item) => `<article class="server-card server-card-${escape(item.status_class)}${item.oracle ? " server-card-oracle" : ""}"><div class="server-card-top"><div class="server-card-name">${escape(item.label)}</div><div class="server-card-server-time">${escape(item.server_time)}</div></div><div class="server-card-state server-card-state-${escape(item.status_class)}">${escape(item.status)}</div><div class="server-card-detail"><div><div class="server-card-detail-label">응답시간</div><div class="server-card-detail-value">${escape(item.response_ms)}</div></div><div><div class="server-card-detail-label">사용자 세션</div><div class="server-card-detail-value">${escape(item.sessions)}</div></div><div><div class="server-card-detail-label">DATA GB</div><div class="server-card-detail-value">${escape(item.data_gb)}</div></div></div>${(item.trends || []).map(trend).join("")}${item.error ? `<div class="server-card-error server-card-error-${escape(item.status_class)}">${escape(item.error)}</div>` : ""}</article>`
  const table = (rows) => {
    if (!rows.length) return '<div class="empty-tablespace">표시할 테이블스페이스 정보가 없습니다.</div>'
    const body = rows.map((row) => `<tr><td>${escape(row.name)}</td><td>${escape(row.used_percent)}</td><td>${escape(row.total_gb)}</td><td>${escape(row.used_gb)}</td></tr>`).join("")
    return `<table class="tablespace-table"><thead><tr><th>테이블스페이스</th><th>사용률</th><th>전체 GB</th><th>사용 GB</th></tr></thead><tbody>${body}</tbody></table>`
  }
  root.innerHTML = `<div class="board-grid"><section><h2 class="board-section-title">MSSQL</h2><div class="mssql-grid">${(data.sources || []).map(card).join("")}</div></section><section><h2 class="board-section-title">Oracle</h2>${card(data.target)}<h2 class="tablespace-title">Oracle 테이블스페이스</h2>${table(data.tablespaces || [])}</section></div>`
}
"""

SITUATION_BOARD = st.components.v2.component(
    "mssql_oracle_situation_board",
    html=SITUATION_BOARD_HTML,
    css=SITUATION_BOARD_CSS,
    js=SITUATION_BOARD_JS,
)


def clean_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def error_text(exc: Exception) -> str:
    text = clean_text(exc)
    return text.split("\n", 1)[0][:180] or "조회할 수 없습니다."


def is_auth_error(text: str) -> bool:
    normalized = clean_text(text).upper()
    markers = ("28000", "18456", "LOGIN FAILED", "로그인 실패", "ORA-01017", "ORA-01045", "ORA-28000")
    return any(marker.upper() in normalized for marker in markers)


def oracle_config() -> dict[str, Any]:
    required = ("user", "password", "host", "port", "service_name")
    if "oracle" not in st.secrets:
        raise ValueError(".streamlit/secrets.toml에 [oracle] 설정이 없습니다.")
    config = dict(st.secrets["oracle"])
    missing = [key for key in required if not clean_text(config.get(key))]
    if missing:
        raise ValueError(f"[oracle] 필수 항목이 없습니다: {', '.join(missing)}")
    return config


def mssql_config() -> dict[str, Any]:
    required = ("host", "port", "user", "password")
    if "mssql" not in st.secrets:
        raise ValueError(".streamlit/secrets.toml에 [mssql] 설정이 없습니다.")
    config = dict(st.secrets["mssql"])
    if not clean_text(config.get("driver")):
        config["driver"] = config.get("dirver", "")
    missing = [key for key in required + ("driver",) if not clean_text(config.get(key))]
    if missing:
        raise ValueError(f"[mssql] 필수 항목이 없습니다: {', '.join(missing)}")
    return config


def mssql_connection(config: dict[str, Any], database: str | None = None) -> Any:
    if pyodbc is None:
        raise RuntimeError("pyodbc가 설치되지 않았습니다. requirements.txt 설치 후 다시 실행하십시오.")
    database_clause = f"DATABASE={database};" if database else ""
    connection_string = (
        f"DRIVER={{{clean_text(config['driver'])}}};"
        f"SERVER={clean_text(config['host'])},{clean_text(config['port'])};"
        f"{database_clause}UID={clean_text(config['user'])};PWD={clean_text(config['password'])};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(connection_string, timeout=10)


@st.cache_resource(show_spinner=False)
def get_oracle_pool(host: str, port: int, service_name: str, user: str, password: str) -> oracledb.ConnectionPool:
    return oracledb.create_pool(
        user=user,
        password=password,
        dsn=f"{host}:{port}/{service_name}",
        min=1,
        max=2,
        increment=1,
    )


def shared_monitor_store() -> dict[str, Any]:
    if "mssql_oracle_monitor_store" not in st.session_state:
        st.session_state["mssql_oracle_monitor_store"] = {
            "snapshot": None,
            "session_history": {name: [] for name in SOURCE_SERVERS + ORACLE_SESSION_USERS},
        }
    return st.session_state["mssql_oracle_monitor_store"]


def empty_mssql_result(name: str, label: str, error: str) -> dict[str, Any]:
    status = "설정 오류" if "설정" in error or "필수" in error else "연결 실패"
    return {
        "system": name,
        "label": label,
        "status": status,
        "auth_failed": False,
        "response_ms": None,
        "server": "-",
        "database": "-",
        "version": "-",
        "server_time": "-",
        "sessions": None,
        "session_users": {name: None for name in ORACLE_SESSION_USERS},
        "data_gb": None,
        "log_gb": None,
        "error": error,
    }


def monitor_mssql_databases(config: dict[str, Any]) -> list[dict[str, Any]]:
    started = perf_counter()
    try:
        with mssql_connection(config) as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT @@SERVERNAME AS server_name,
                       CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)) AS version,
                       CONVERT(varchar(19), SYSDATETIME(), 120) AS server_time
                """
            )
            columns = [column[0] for column in cursor.description]
            server_row = dict(zip(columns, cursor.fetchone()))
            cursor.execute(
                """
                SELECT name, state_desc
                  FROM sys.databases
                 WHERE name IN ('CERDB', 'INSIDEBANK', 'JBNDB', 'MEMDB', 'PREEDDB', 'SALDB')
                 ORDER BY name
                """
            )
            database_states = {clean_text(row[0]).upper(): clean_text(row[1]).upper() for row in cursor.fetchall()}
            results = []
            for name in SOURCE_SERVERS:
                result = empty_mssql_result(name, name, "")
                result.update(
                    {
                        "server": clean_text(server_row["server_name"]) or "MSSQL",
                        "version": clean_text(server_row["version"]),
                        "server_time": clean_text(server_row["server_time"]),
                    }
                )
                database_state = database_states.get(name)
                if not database_state:
                    result.update({"status": "DB 없음", "error": "서버 카탈로그에 없습니다."})
                    results.append(result)
                    continue
                if database_state != "ONLINE":
                    result.update({"status": database_state, "error": ""})
                    results.append(result)
                    continue
                database_started = perf_counter()
                try:
                    cursor.execute(f"USE [{name}]")
                    cursor.execute("SELECT DB_NAME() AS database_name")
                    database_name = clean_text(cursor.fetchone()[0]) or name
                    cursor.execute(
                        """
                        SELECT COUNT(*)
                          FROM sys.dm_exec_sessions
                         WHERE is_user_process = 1
                           AND database_id = DB_ID()
                        """
                    )
                    session_count = int(cursor.fetchone()[0])
                    cursor.execute(
                        """
                        SELECT
                            CAST(COALESCE(SUM(CASE WHEN type_desc = 'ROWS' THEN size ELSE 0 END), 0) * 8.0 / 1024 / 1024 AS decimal(18, 2)) AS data_gb,
                            CAST(COALESCE(SUM(CASE WHEN type_desc = 'LOG' THEN size ELSE 0 END), 0) * 8.0 / 1024 / 1024 AS decimal(18, 2)) AS log_gb
                          FROM sys.database_files
                        """
                    )
                    size_row = cursor.fetchone()
                    result.update(
                        {
                            "status": "ONLINE",
                            "response_ms": round((perf_counter() - database_started) * 1000),
                            "database": database_name,
                            "sessions": session_count,
                            "data_gb": float(size_row[0] or 0),
                            "log_gb": float(size_row[1] or 0),
                            "error": "",
                        }
                    )
                except Exception as exc:
                    result["status"] = "조회 실패"
                    result["error"] = error_text(exc)
                results.append(result)
            return results
    except Exception as exc:
        error = error_text(exc)
        results = [empty_mssql_result(name, name, error) for name in SOURCE_SERVERS]
        if is_auth_error(error):
            for result in results:
                result["status"] = "인증 실패"
                result["auth_failed"] = True
        return results


def empty_oracle_result(error: str = "") -> dict[str, Any]:
    return {
        "system": "ORACLE",
        "label": "Oracle",
        "status": "연결 실패",
        "auth_failed": False,
        "response_ms": None,
        "server": "-",
        "database": "-",
        "version": "-",
        "server_time": "-",
        "sessions": None,
        "session_users": {name: None for name in ORACLE_SESSION_USERS},
        "data_gb": None,
        "tablespaces": pd.DataFrame(columns=["테이블스페이스", "사용률", "전체 GB", "사용 GB"]),
        "error": error,
    }


def monitor_oracle_server() -> dict[str, Any]:
    started = perf_counter()
    result = empty_oracle_result()
    try:
        config = oracle_config()
        pool = get_oracle_pool(
            clean_text(config["host"]),
            int(config["port"]),
            clean_text(config["service_name"]),
            clean_text(config["user"]),
            clean_text(config["password"]),
        )
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT SYS_CONTEXT('USERENV', 'DB_NAME') AS database_name,
                           USER AS current_user,
                           TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS server_time
                      FROM DUAL
                    """
                )
                columns = [column[0] for column in cursor.description]
                row = dict(zip(columns, cursor.fetchone()))
                result.update(
                    {
                        "status": "ONLINE",
                        "response_ms": round((perf_counter() - started) * 1000),
                        "database": clean_text(row["DATABASE_NAME"]) or "ORACLE",
                        "server_time": clean_text(row["SERVER_TIME"]),
                    }
                )
                try:
                    cursor.execute("SELECT instance_name, version FROM v$instance")
                    columns = [column[0] for column in cursor.description]
                    instance_row = dict(zip(columns, cursor.fetchone()))
                    result["server"] = clean_text(instance_row["INSTANCE_NAME"]) or result["database"]
                    result["version"] = clean_text(instance_row["VERSION"])
                except Exception as exc:
                    result["server"] = result["database"]
                    result["error"] = f"인스턴스: {error_text(exc)}"
                try:
                    cursor.execute(
                        """
                        SELECT username, COUNT(*) AS session_count
                          FROM v$session
                         WHERE type = 'USER'
                           AND username IN (:owner_1, :owner_2)
                         GROUP BY username
                        """,
                        {"owner_1": ORACLE_SESSION_USERS[0], "owner_2": ORACLE_SESSION_USERS[1]},
                    )
                    session_users = {name: 0 for name in ORACLE_SESSION_USERS}
                    for username, session_count in cursor.fetchall():
                        name = clean_text(username).upper()
                        if name in session_users:
                            session_users[name] = int(session_count)
                    result["session_users"] = session_users
                    result["sessions"] = sum(session_users.values())
                except Exception as exc:
                    detail = f"사용자 세션: {error_text(exc)}"
                    result["error"] = " · ".join(value for value in (result["error"], detail) if value)
                try:
                    cursor.execute(
                        """
                        SELECT ROUND(SUM(usage.used_space * spaces.block_size) / POWER(1024, 3), 2) AS used_gb
                          FROM dba_tablespace_usage_metrics usage
                          JOIN dba_tablespaces spaces ON spaces.tablespace_name = usage.tablespace_name
                        """
                    )
                    result["data_gb"] = float(cursor.fetchone()[0] or 0)
                except Exception as exc:
                    detail = f"전체 테이블스페이스 사용량: {error_text(exc)}"
                    result["error"] = " · ".join(value for value in (result["error"], detail) if value)
                try:
                    cursor.execute(
                        """
                        SELECT usage.tablespace_name,
                               usage.used_percent,
                               ROUND(usage.tablespace_size * spaces.block_size / POWER(1024, 3), 2) AS total_gb,
                               ROUND(usage.used_space * spaces.block_size / POWER(1024, 3), 2) AS used_gb
                          FROM dba_tablespace_usage_metrics usage
                          JOIN dba_tablespaces spaces ON spaces.tablespace_name = usage.tablespace_name
                         WHERE usage.tablespace_name IN ('PCERP_DATA', 'PCERP_INDEX', 'PCERP_MIG_DATA', 'PCERP_MIG_INDEX')
                            OR spaces.contents = 'UNDO'
                         ORDER BY usage.tablespace_name
                        """
                    )
                    columns = [column[0] for column in cursor.description]
                    result["tablespaces"] = pd.DataFrame(cursor.fetchall(), columns=columns).rename(
                        columns={"TABLESPACE_NAME": "테이블스페이스", "USED_PERCENT": "사용률", "TOTAL_GB": "전체 GB", "USED_GB": "사용 GB"}
                    )
                except Exception as exc:
                    detail = f"테이블스페이스: {error_text(exc)}"
                    result["error"] = " · ".join(value for value in (result["error"], detail) if value)
    except Exception as exc:
        result["error"] = error_text(exc)
        if is_auth_error(result["error"]):
            result["status"] = "인증 실패"
            result["auth_failed"] = True
    return result


def load_snapshot(previous: dict[str, Any] | None) -> dict[str, Any]:
    previous_sources = {result["system"]: result for result in (previous or {}).get("sources", [])}
    source_auth_failed = any(result.get("auth_failed") for result in previous_sources.values())
    if source_auth_failed:
        sources = [previous_sources.get(name, empty_mssql_result(name, name, "MSSQL 인증 실패로 자동 조회를 중지했습니다.")) for name in SOURCE_SERVERS]
    else:
        try:
            config = mssql_config()
            sources = monitor_mssql_databases(config)
        except Exception as exc:
            sources = [empty_mssql_result(name, name, error_text(exc)) for name in SOURCE_SERVERS]
    previous_target = (previous or {}).get("target")
    if previous_target and previous_target.get("auth_failed"):
        target = previous_target
    else:
        target = monitor_oracle_server()
    return {"checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "sources": sources, "target": target}


def refresh_snapshot() -> dict[str, Any]:
    store = shared_monitor_store()
    store["snapshot"] = load_snapshot(store["snapshot"])
    history = store["session_history"]
    for monitor in store["snapshot"]["sources"]:
        if monitor["status"] == "ONLINE" and monitor["sessions"] is not None:
            values = history.setdefault(monitor["system"], [])
            values.append(int(monitor["sessions"]))
            history[monitor["system"]] = values[-60:]
    target = store["snapshot"]["target"]
    if not target["auth_failed"]:
        for name, session_count in target["session_users"].items():
            if session_count is not None:
                values = history.setdefault(name, [])
                values.append(int(session_count))
                history[name] = values[-60:]
    return store["snapshot"]


def apply_situation_board_style() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stAppViewContainer"] { background: radial-gradient(circle at 18% -10%, #17385f 0, #0a1324 34%, #07101e 100%); }
        [data-testid="stHeader"] { background: transparent; }
        .block-container { max-width: 1900px; padding: .8rem 1rem 1rem; }
        .board-title { color: #f4f8fc; font-size: 2.2rem; font-weight: 760; letter-spacing: -.035em; line-height: 1.05; }
        .board-author { color: #75c7ff; font-size: .78rem; font-weight: 700; margin-top: .28rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def number_label(value: Any, suffix: str = "") -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):,.2f}{suffix}" if isinstance(value, float) else f"{int(value):,}{suffix}"


def data_capacity_label(value: Any, include_tb: bool = False) -> str:
    if value is None or pd.isna(value):
        return "-"
    gb_value = float(value)
    if include_tb:
        return f"{gb_value:,.2f} GB [{gb_value / 1024:,.2f} TB]"
    return f"{gb_value:,.2f}"


def server_datetime_label(value: Any) -> str:
    return clean_text(value) or "-"


def status_class(status: str) -> str:
    if status == "ONLINE":
        return "ok"
    if status == "인증 실패":
        return "auth"
    if status == "설정 오류":
        return "setup"
    if status in ("연결 실패", "조회 실패"):
        return "fail"
    return "wait"


def format_number(value: Any, decimals: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):,.{decimals}f}"


def formatted_tablespace_frame(frame: pd.DataFrame) -> pd.DataFrame:
    formatted = frame.copy()
    formatted["사용률"] = formatted["사용률"].map(lambda value: f"{float(value):,.2f}%" if pd.notna(value) else "-")
    formatted["전체 GB"] = formatted["전체 GB"].map(lambda value: data_capacity_label(value, include_tb=True))
    formatted["사용 GB"] = formatted["사용 GB"].map(lambda value: data_capacity_label(value, include_tb=True))
    return formatted


def situation_card_payload(monitor: dict[str, Any], session_history: dict[str, list[int]]) -> dict[str, Any]:
    error = "MSSQL 인증 실패로 자동 조회를 차단했습니다." if monitor["auth_failed"] else clean_text(monitor["error"])
    if monitor["system"] in SOURCE_SERVERS:
        trends = [{"name": "세션", "value": number_label(monitor["sessions"]), "history": session_history.get(monitor["system"], [])}]
    else:
        trends = [
            {"name": name, "value": number_label(monitor["session_users"].get(name)), "history": session_history.get(name, [])}
            for name in ORACLE_SESSION_USERS
        ]
    return {
        "label": clean_text(monitor["label"]),
        "status": clean_text(monitor["status"]),
        "status_class": status_class(monitor["status"]),
        "response_ms": number_label(monitor["response_ms"], " ms"),
        "sessions": number_label(monitor["sessions"]),
        "data_gb": data_capacity_label(monitor["data_gb"], include_tb=True),
        "server_time": server_datetime_label(monitor["server_time"]),
        "error": error,
        "oracle": monitor["system"] == "ORACLE",
        "trends": trends,
    }


def situation_board_payload(snapshot: dict[str, Any], session_history: dict[str, list[int]]) -> dict[str, Any]:
    tablespaces = formatted_tablespace_frame(snapshot["target"]["tablespaces"])
    table_rows = [
        {
            "name": clean_text(row["테이블스페이스"]),
            "used_percent": clean_text(row["사용률"]),
            "total_gb": clean_text(row["전체 GB"]),
            "used_gb": clean_text(row["사용 GB"]),
        }
        for _, row in tablespaces.iterrows()
    ]
    return {
        "sources": [situation_card_payload(monitor, session_history) for monitor in snapshot["sources"]],
        "target": situation_card_payload(snapshot["target"], session_history),
        "tablespaces": table_rows,
    }


def render_situation_board(snapshot: dict[str, Any], session_history: dict[str, list[int]]) -> None:
    SITUATION_BOARD(data=situation_board_payload(snapshot, session_history), key="mssql-oracle-situation-board")


def render_live_board(refresh_seconds: int) -> None:
    @st.fragment(run_every=refresh_seconds)
    def live_board() -> None:
        store = shared_monitor_store()
        if store["snapshot"] is None:
            with st.spinner("서버 상태를 갱신하고 있습니다."):
                snapshot = refresh_snapshot()
        else:
            snapshot = refresh_snapshot()
        session_history = shared_monitor_store()["session_history"]
        render_situation_board(snapshot, session_history)

    live_board()


def main() -> None:
    st.set_page_config(page_title="IDC Server Monitoring", page_icon=":material/monitoring:", layout="wide")
    apply_situation_board_style()
    title_area, interval_area = st.columns([7, 2], vertical_alignment="center")
    title_area.markdown('<div class="board-title">IDC Server Monitoring</div><div class="board-author">⚙️ Created by ♡홍율파파</div>', unsafe_allow_html=True)
    refresh_label = interval_area.selectbox("조회 간격", list(REFRESH_OPTIONS), index=1)
    render_live_board(REFRESH_OPTIONS[refresh_label])


if __name__ == "__main__":
    main()

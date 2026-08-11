import re
from datetime import datetime
from typing import List, Tuple

import oracledb
import streamlit as st


DEFAULT_TABLE_TS = "PCERP_MIG_DATA"
DEFAULT_INDEX_TS = "PCERP_MIG_INDEX"
ORACLE_DEFAULT_OWNERS = {
    "ANONYMOUS",
    "APPQOSSYS",
    "AUDSYS",
    "CTXSYS",
    "DBSNMP",
    "DIP",
    "DVF",
    "DVSYS",
    "GGSYS",
    "GSMADMIN_INTERNAL",
    "GSMCATUSER",
    "GSMUSER",
    "LBACSYS",
    "MDSYS",
    "OJVMSYS",
    "OLAPSYS",
    "ORACLE_OCM",
    "OUTLN",
    "REMOTE_SCHEDULER_AGENT",
    "SI_INFORMTN_SCHEMA",
    "SYS",
    "SYS$UMF",
    "SYSBACKUP",
    "SYSDG",
    "SYSKM",
    "SYSMAN",
    "SYSTEM",
    "WMSYS",
    "XDB",
    "XS$NULL",
}


def init_session_state() -> None:
    defaults = {
        "combined_ddl": None,
        "check_ddl_part": None,
        "drop_ddl_part": None,
        "body_ddl_part": None,
        "index_ddl_part": None,
        "stats_ddl_part": None,
        "error_logs": [],
        "last_success_count": 0,
        "last_fail_count": 0,
        "last_total_count": 0,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


@st.cache_resource(show_spinner=False)
def get_pool() -> oracledb.ConnectionPool:
    conf = st.secrets["oracle"]
    dsn = f"{conf['host']}:{conf['port']}/{conf['service_name']}"
    return oracledb.create_pool(
        user=conf["user"],
        password=conf["password"],
        dsn=dsn,
        min=1,
        max=4,
        increment=1,
    )


@st.cache_data(ttl=300, show_spinner=False)
def get_owner_options() -> Tuple[str, List[str]]:
    pool = get_pool()
    with pool.acquire() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT USER FROM DUAL")
            row = cursor.fetchone()
            login_owner = str(row[0]).strip().upper() if row and row[0] else "USER"
            cursor.execute("SELECT username FROM all_users ORDER BY username")
            owners = [
                str(row[0]).strip().upper()
                for row in cursor.fetchall()
                if row[0] and str(row[0]).strip().upper() not in ORACLE_DEFAULT_OWNERS
            ]

    if login_owner not in owners:
        owners.insert(0, login_owner)
    if not owners:
        owners = [login_owner]
    return login_owner, owners


def clean_ddl_text(text: str) -> str:
    patterns = [
        r"PCTFREE\s+\d+",
        r"PCTUSED\s+\d+",
        r"INITRANS\s+\d+",
        r"MAXTRANS\s+\d+",
        r"NOCOMPRESS",
        r"COMPUTE\s+STATISTICS",
        r"SEGMENT\s+CREATION\s+(IMMEDIATE|DEFERRED)",
        r"BUFFER_POOL\s+[A-Za-z0-9_]+",
        r"FLASH_CACHE\s+[A-Za-z0-9_]+",
        r"CELL_FLASH_CACHE\s+[A-Za-z0-9_]+",
        r"PCTINCREASE\s+\d+",
        r"FREELISTS\s+\d+",
        r"FREELIST\s+GROUPS\s+\d+",
    ]
    for pattern in patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)

    storage_pattern = re.compile(r"STORAGE\s*\(", flags=re.IGNORECASE)
    while True:
        match = storage_pattern.search(text)
        if not match:
            break
        start = match.start()
        idx = match.end() - 1
        depth = 0
        end = None
        for i in range(idx, len(text)):
            if text[i] == "(":
                depth += 1
            elif text[i] == ")":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end is None:
            break
        text = text[:start] + text[end + 1 :]

    text = text.replace('"', "")
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def split_owner_and_table(
    cursor: oracledb.Cursor,
    table_input: str,
    selected_owner: str,
) -> Tuple[str, str]:
    full_name = table_input.strip().upper()
    if "." in full_name:
        owner, table_name = full_name.split(".", 1)
        return owner.strip(), table_name.strip()

    return selected_owner.strip().upper(), full_name


def set_metadata_transform(cursor: oracledb.Cursor) -> None:
    cursor.execute(
        """
        BEGIN
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', TRUE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', TRUE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS_AS_ALTER', FALSE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
        END;
        """
    )


def format_table_structure(ddl_text: str, ts_name: str, index_ts_name: str) -> str:
    upper_ts = ts_name.strip().upper()
    upper_index_ts = index_ts_name.strip().upper()
    first_open = ddl_text.find("(")
    if first_open == -1:
        return ddl_text

    paren_depth = 0
    main_close_idx = -1
    for i in range(first_open, len(ddl_text)):
        if ddl_text[i] == "(":
            paren_depth += 1
        elif ddl_text[i] == ")":
            paren_depth -= 1
            if paren_depth == 0:
                main_close_idx = i
                break
    if main_close_idx == -1:
        return ddl_text

    header = ddl_text[:first_open].strip() + " ("
    body = ddl_text[first_open + 1 : main_close_idx]
    tail = ddl_text[main_close_idx + 1 :].strip()

    items: List[str] = []
    current: List[str] = []
    sub_depth = 0
    for char in body:
        if char == "(":
            sub_depth += 1
            current.append(char)
        elif char == ")":
            sub_depth -= 1
            current.append(char)
        elif char == "," and sub_depth == 0:
            item = "".join(current).strip().lstrip(",").strip()
            if item:
                items.append(item)
            current = []
        else:
            current.append(char)
    tail_item = "".join(current).strip().lstrip(",").strip()
    if tail_item:
        items.append(tail_item)

    body_lines: List[str] = []
    for item in items:
        if "CONSTRAINT " in item.upper() and "PRIMARY KEY" in item.upper():
            item = re.sub(
                r"TABLESPACE\s+[A-Za-z0-9_\.]+",
                f"TABLESPACE {upper_index_ts}",
                item,
                flags=re.IGNORECASE,
            )
        prefix = "  " if not body_lines else ", "
        body_lines.append(f"{prefix}{item}")

    final_table = header + "\n" + "\n".join(body_lines) + "\n)"

    tail = re.sub(r"TABLESPACE\s+[A-Za-z0-9_\.]+", "", tail, flags=re.IGNORECASE)
    tail = re.sub(r"NOLOGGING", "", tail, flags=re.IGNORECASE)
    tail = re.sub(r"LOGGING", "", tail, flags=re.IGNORECASE)
    tail = re.sub(r"\s{2,}", " ", tail).strip()
    if tail.endswith(";"):
        tail = tail[:-1].strip()

    if "LOB (" in tail.upper() or "LOB(" in tail.upper():
        tail = re.sub(
            r"TABLESPACE\s+[A-Za-z0-9_\.]+",
            f"TABLESPACE {upper_ts}",
            tail,
            flags=re.IGNORECASE,
        )
        return f"{final_table}\nNOLOGGING TABLESPACE {upper_ts}\n{tail};"

    return f"{final_table}\nNOLOGGING TABLESPACE {upper_ts};"


def format_index_line(idx_text: str, ts_name: str, parallel: bool = False) -> str:
    idx_text = re.sub(r"TABLESPACE\s+[A-Za-z0-9_\.]+", "", idx_text, flags=re.IGNORECASE)
    idx_text = re.sub(r"NOLOGGING", "", idx_text, flags=re.IGNORECASE)
    idx_text = re.sub(r"LOGGING", "", idx_text, flags=re.IGNORECASE)
    idx_text = re.sub(r"\bNOPARALLEL\b", "", idx_text, flags=re.IGNORECASE)
    idx_text = re.sub(r"\bPARALLEL(?:\s+\d+)?\b", "", idx_text, flags=re.IGNORECASE)
    idx_text = re.sub(r"\s{2,}", " ", idx_text).strip()
    if idx_text.endswith(";"):
        idx_text = idx_text[:-1].strip()
    suffix = f"TABLESPACE {ts_name.strip().upper()}"
    if parallel:
        suffix += " NOLOGGING PARALLEL 4"
    return f"{idx_text} {suffix};"


def read_lob_text(value: object) -> str:
    return value.read() if hasattr(value, "read") else str(value)


def fetch_table_ddl(
    cursor: oracledb.Cursor,
    owner: str,
    table_name: str,
    table_ts: str,
    index_ts: str,
) -> str:
    cursor.execute(
        "SELECT DBMS_METADATA.GET_DDL('TABLE', :table_name, :owner) FROM DUAL",
        {"table_name": table_name, "owner": owner},
    )
    row = cursor.fetchone()
    if not row or not row[0]:
        raise ValueError("테이블 DDL 추출 실패")

    raw = clean_ddl_text(read_lob_text(row[0]))
    combined_table_name = f"{owner}.{table_name}"
    table_ddl = format_table_structure(raw, table_ts, index_ts)
    return re.sub(
        r"(CREATE\s+TABLE\s+)(?:[A-Za-z0-9_]+\.)?([A-Za-z0-9_]+)",
        rf"\1{combined_table_name}",
        table_ddl,
        flags=re.IGNORECASE,
    )


def fetch_comment_block(cursor: oracledb.Cursor, owner: str, table_name: str) -> str:
    combined = f"{owner}.{table_name}"
    lines: List[str] = []

    cursor.execute(
        """
        SELECT comments
          FROM all_tab_comments
         WHERE owner = :owner
           AND table_name = :table_name
        """,
        {"owner": owner, "table_name": table_name},
    )
    row = cursor.fetchone()
    if row and row[0]:
        t_comment = str(row[0]).replace("'", "''")
        lines.append(f"COMMENT ON TABLE {combined} IS '{t_comment}';")

    cursor.execute(
        """
        SELECT c.column_name, c.comments
          FROM all_col_comments c
          JOIN all_tab_columns tc
            ON tc.owner = c.owner
           AND tc.table_name = c.table_name
           AND tc.column_name = c.column_name
         WHERE c.owner = :owner
           AND c.table_name = :table_name
           AND c.comments IS NOT NULL
         ORDER BY tc.column_id
        """,
        {"owner": owner, "table_name": table_name},
    )
    for c_name, c_comment in cursor.fetchall():
        safe_comment = str(c_comment).replace("'", "''").strip()
        lines.append(f"COMMENT ON COLUMN {combined}.{str(c_name).strip()} IS '{safe_comment}';")

    return "\n".join(lines)


def fetch_grant_block(cursor: oracledb.Cursor, owner: str, table_name: str) -> str:
    """대상 서버의 사용자·롤 부재에 대비해 권한 DDL을 주석 처리한다."""
    try:
        cursor.execute(
            "SELECT DBMS_METADATA.GET_DEPENDENT_DDL('OBJECT_GRANT', :table_name, :owner) FROM DUAL",
            {"table_name": table_name, "owner": owner},
        )
        row = cursor.fetchone()
        if not row or not row[0]:
            return ""
        grant_raw = read_lob_text(row[0])
        grant_lines = [f"-- {line.strip()}" for line in grant_raw.splitlines() if line.strip()]
        return "\n".join(grant_lines)
    except Exception:
        return ""


def fetch_index_block(
    cursor: oracledb.Cursor,
    owner: str,
    table_name: str,
    index_ts: str,
) -> Tuple[str, str, str, str]:
    cursor.execute(
        """
        SELECT i.owner, i.index_name
          FROM all_indexes i
         WHERE i.table_owner = :owner
           AND i.table_name = :table_name
           AND i.index_type NOT IN ('LOB', 'IOT - TOP')
           AND i.index_name NOT LIKE 'SYS$_%' ESCAPE '$'
           AND NOT EXISTS (
                SELECT 1
                  FROM all_constraints c
                 WHERE c.owner = i.table_owner
                   AND c.table_name = i.table_name
                   AND c.constraint_type IN ('P', 'U')
                   AND c.index_owner = i.owner
                   AND c.index_name = i.index_name
           )
         ORDER BY i.owner, i.index_name
        """,
        {"owner": owner, "table_name": table_name},
    )

    create_statements: List[str] = []
    parallel_create_statements: List[str] = []
    drop_statements: List[str] = []
    noparallel_statements: List[str] = []
    for index_owner, index_name in cursor.fetchall():
        cursor.execute(
            "SELECT DBMS_METADATA.GET_DDL('INDEX', :index_name, :index_owner) FROM DUAL",
            {"index_name": index_name, "index_owner": index_owner},
        )
        row = cursor.fetchone()
        if not row or not row[0]:
            continue

        stmt = clean_ddl_text(read_lob_text(row[0]))
        stmt = re.sub(
            r"(CREATE\s+(?:(?:UNIQUE|BITMAP)\s+)?INDEX\s+)(?:[A-Za-z0-9_]+\.)?([A-Za-z0-9_]+)",
            rf"\1{owner}.\2",
            stmt,
            flags=re.IGNORECASE,
        )
        stmt = re.sub(
            r"(\bON\s+)(?:[A-Za-z0-9_]+\.)?([A-Za-z0-9_]+)",
            rf"\1{owner}.{table_name}",
            stmt,
            flags=re.IGNORECASE,
        )
        create_statements.append(format_index_line(stmt, index_ts))
        parallel_create_statements.append(format_index_line(stmt, index_ts, parallel=True))
        drop_statements.append(f"DROP INDEX {str(index_owner).strip()}.{str(index_name).strip()};")
        noparallel_statements.append(
            f"ALTER INDEX {str(index_owner).strip()}.{str(index_name).strip()} NOPARALLEL;"
        )

    return (
        "\n".join(create_statements),
        "\n".join(drop_statements),
        "\n".join(parallel_create_statements),
        "\n".join(noparallel_statements),
    )


def build_stats_block(owner: str, table_name: str) -> str:
    owner_sql = owner.strip().upper().replace("'", "''")
    table_sql = table_name.strip().upper().replace("'", "''")
    stats = "BEGIN\n"
    stats += "    DBMS_STATS.GATHER_TABLE_STATS(\n"
    stats += f"        ownname          => '{owner_sql}',\n"
    stats += f"        tabname          => '{table_sql}',\n"
    stats += "        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,\n"
    stats += "        method_opt       => 'FOR ALL COLUMNS SIZE AUTO',\n"
    stats += "        degree           => 4,\n"
    stats += "        cascade          => TRUE\n"
    stats += "    );\n"
    stats += "END;\n/"
    return stats


def generate_ddl_components(
    cursor: oracledb.Cursor,
    table_input: str,
    selected_owner: str,
    table_ts: str,
    index_ts: str,
) -> Tuple[bool, str, str, str, str, str]:
    try:
        owner, table_name = split_owner_and_table(cursor, table_input, selected_owner)
        table_block = fetch_table_ddl(cursor, owner, table_name, table_ts, index_ts)
        index_block, index_drop_block, parallel_index_block, index_noparallel_block = fetch_index_block(
            cursor,
            owner,
            table_name,
            index_ts,
        )
        comment_block = fetch_comment_block(cursor, owner, table_name)
        grant_block = fetch_grant_block(cursor, owner, table_name)
        stats_block = build_stats_block(owner, table_name)

        parts = [table_block]
        if index_block:
            parts.append(index_block)
        if comment_block:
            parts.append(comment_block)
        if grant_block:
            parts.append(grant_block)

        return (
            True,
            "\n\n".join(parts) + "\n",
            index_drop_block,
            parallel_index_block,
            index_noparallel_block,
            stats_block,
        )
    except Exception as exc:
        return False, str(exc).strip(), "", "", "", ""


def build_check_part(table_names: List[str], selected_owner: str) -> str:
    owner_sql = selected_owner.strip().upper().replace("'", "''")
    quoted_tables = ", ".join([f"'{name.replace(chr(39), chr(39) * 2)}'" for name in table_names])
    check = "/******************************************/\n"
    check += "/* [STAGE 1] SELECT TABLE COUNT & SIZE */\n"
    check += "/******************************************/\n"
    check += "BEGIN\n    DBMS_STATS.FLUSH_DATABASE_MONITORING_INFO;\nEND;\n/\n\n"
    check += "SELECT T.OWNER\n"
    check += "     , T.TABLE_NAME\n"
    check += "     , C.COMMENTS\n"
    check += "     , (NVL(T.NUM_ROWS, 0) + NVL(M.INSERTS, 0) - NVL(M.DELETES, 0)) AS TBL_CNT\n"
    check += "     , NVL(S.TBL_SIZ_MB, 0) AS TBL_SIZ_MB\n"
    check += "     , NVL(S.TBL_SIZ_GB, 0) AS TBL_SIZ_GB\n"
    check += "  FROM ALL_TABLES T\n"
    check += "  LEFT JOIN ALL_TAB_COMMENTS C ON T.OWNER = C.OWNER AND T.TABLE_NAME = C.TABLE_NAME\n"
    check += "  LEFT JOIN ALL_TAB_MODIFICATIONS M ON T.OWNER = M.TABLE_OWNER AND T.TABLE_NAME = M.TABLE_NAME\n"
    check += "  LEFT JOIN (\n"
    check += "        SELECT NVL(L.TABLE_NAME, S.SEGMENT_NAME) AS TABLE_NAME\n"
    check += "             , ROUND(SUM(S.BYTES) / 1024 / 1024, 2) AS TBL_SIZ_MB\n"
    check += "             , ROUND(SUM(S.BYTES) / 1024 / 1024 / 1024, 2) AS TBL_SIZ_GB\n"
    check += "          FROM ALL_SEGMENTS S\n"
    check += "          LEFT JOIN ALL_LOBS L\n"
    check += "            ON L.OWNER = S.OWNER\n"
    check += "           AND L.SEGMENT_NAME = S.SEGMENT_NAME\n"
    check += f"         WHERE S.OWNER = '{owner_sql}'\n"
    check += "           AND S.SEGMENT_TYPE IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION', 'LOBSEGMENT', 'LOB PARTITION', 'LOB SUBPARTITION')\n"
    check += "         GROUP BY NVL(L.TABLE_NAME, S.SEGMENT_NAME)\n"
    check += "  ) S ON T.TABLE_NAME = S.TABLE_NAME\n"
    check += " WHERE T.IOT_TYPE IS NULL\n"
    check += f"   AND T.OWNER = '{owner_sql}'\n"
    check += f"   AND T.TABLE_NAME IN ({quoted_tables})\n"
    check += " ORDER BY TBL_SIZ_MB DESC\n"
    check += ";\n"
    return check


def build_drop_part(table_names: List[str], selected_owner: str) -> str:
    owner_sql = selected_owner.strip().upper().replace("'", "''")
    quoted_tables = ", ".join([f"'{name.replace(chr(39), chr(39) * 2)}'" for name in table_names])
    drop = "/******************************************/\n"
    drop += "/* [STAGE 2] DROP TABLES */\n"
    drop += "/******************************************/\n"
    drop += "BEGIN\n"
    drop += f"    FOR R IN (SELECT OWNER, TABLE_NAME FROM ALL_TABLES WHERE OWNER = '{owner_sql}' AND TABLE_NAME IN ({quoted_tables})) LOOP\n"
    drop += "        EXECUTE IMMEDIATE 'DROP TABLE ' || R.OWNER || '.' || R.TABLE_NAME || ' CASCADE CONSTRAINTS';\n"
    drop += "    END LOOP;\n"
    drop += "END;\n/\n"
    return drop


def render_styles() -> None:
    st.markdown(
        """
        <style>
        .block-container { padding-top: 1rem !important; padding-bottom: 1rem !important; max-width: 1500px; }
        .main-banner {
            background: linear-gradient(135deg, #1e1e38 0%, #3b2d54 100%);
            padding: 18px 24px; border-radius: 8px; color: white; margin-bottom: 15px; box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        }
        .banner-title { font-size: 20px; font-weight: 700; margin: 0 0 4px 0; font-family: 'Malgun Gothic', sans-serif; }
        .banner-creator { font-size: 11px; color: #cbd5e0; font-family: 'Malgun Gothic', sans-serif; }
        html, body, [data-testid="stMarkdownContainer"], p, li { font-family: 'Malgun Gothic', sans-serif !important; font-size: 13.5px !important; }
        code, pre { font-size: 12.5px !important; }
        h5, h3, .stSubheader { font-size: 15px !important; font-weight: bold !important; margin: 10px 0 5px 0 !important; }
        [data-testid="stAlert"] { padding: 8px 12px !important; font-size: 13px !important; }
        hr { margin: 15px 0 !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def run_generation(
    target_tables_input: str,
    selected_owner: str,
    table_ts: str,
    index_ts: str,
) -> None:
    table_inputs = [
        t.strip().upper()
        for t in target_tables_input.replace(",", " ").replace("\n", " ").split()
        if t.strip()
    ]
    if not table_inputs:
        st.warning("테이블 이름을 입력해 주세요.")
        return

    total = len(table_inputs)
    body_list: List[str] = []
    index_drop_list: List[str] = []
    index_create_list: List[str] = []
    index_noparallel_list: List[str] = []
    stats_list: List[str] = []
    errors: List[str] = []
    plain_table_names = [name.split(".")[-1] for name in table_inputs]

    status_text = st.empty()
    progress = st.progress(0.0)

    pool = get_pool()
    with pool.acquire() as conn:
        with conn.cursor() as cursor:
            set_metadata_transform(cursor)
            for idx, table_input in enumerate(table_inputs, start=1):
                status_text.markdown(f"⏳ 진행 중: `{table_input}` ({idx}/{total})")
                progress.progress(idx / total)

                (
                    ok,
                    result,
                    index_drop_block,
                    index_create_block,
                    index_noparallel_block,
                    stats_block,
                ) = generate_ddl_components(
                    cursor,
                    table_input,
                    selected_owner,
                    table_ts,
                    index_ts,
                )
                if ok:
                    body_list.append(result)
                    if index_drop_block:
                        index_drop_list.append(index_drop_block)
                    if index_create_block:
                        index_create_list.append(index_create_block)
                    if index_noparallel_block:
                        index_noparallel_list.append(index_noparallel_block)
                    if stats_block:
                        stats_list.append(stats_block)
                else:
                    now_str = datetime.now().strftime("%H:%M:%S")
                    errors.append(f"[{now_str}] {table_input} - {result}")

    status_text.empty()
    progress.empty()

    st.session_state.error_logs = errors
    st.session_state.last_total_count = total
    st.session_state.last_success_count = len(body_list)
    st.session_state.last_fail_count = len(errors)

    if not body_list:
        st.session_state.check_ddl_part = None
        st.session_state.drop_ddl_part = None
        st.session_state.body_ddl_part = None
        st.session_state.index_ddl_part = None
        st.session_state.stats_ddl_part = None
        st.session_state.combined_ddl = None
        return

    check_part = build_check_part(plain_table_names, selected_owner)
    drop_part = build_drop_part(plain_table_names, selected_owner)
    body_part = "/******************************************/\n"
    body_part += "/* [STAGE 3] CREATE STRUCTURES & METADATA */\n"
    body_part += "/******************************************/\n\n"
    body_part += "\n\n/******************************************/\n\n".join(body_list)
    index_part = "/******************************************/\n"
    index_part += "/* DROP INDEX */\n"
    index_part += "/******************************************/\n"
    index_part += "\n".join(index_drop_list)
    index_part += "\n\n/******************************************/\n"
    index_part += "/* CREATE INDEX */\n"
    index_part += "/******************************************/\n"
    index_part += "ALTER SESSION ENABLE PARALLEL DDL;\n\n"
    index_part += "\n".join(index_create_list)
    index_part += "\n\n/******************************************/\n"
    index_part += "/* ALTER INDEX NOPARALLEL */\n"
    index_part += "/******************************************/\n"
    index_part += "\n".join(index_noparallel_list)
    stats_part = "/******************************************/\n"
    stats_part += "/* GATHER TABLE STATS */\n"
    stats_part += "/******************************************/\n"
    stats_part += "/* 데이터 적재 후에만 실행 */\n"
    stats_part += "\n\n".join(stats_list)

    st.session_state.check_ddl_part = check_part
    st.session_state.drop_ddl_part = drop_part
    st.session_state.body_ddl_part = body_part
    st.session_state.index_ddl_part = index_part
    st.session_state.stats_ddl_part = stats_part
    st.session_state.combined_ddl = check_part + "\n\n\n" + drop_part + "\n\n\n" + body_part


def render_result(show_preview: bool) -> None:
    total = st.session_state.last_total_count
    success = st.session_state.last_success_count
    fail = st.session_state.last_fail_count

    c1, c2, c3 = st.columns(3)
    c1.metric("총 테이블", total)
    c2.metric("성공", success)
    c3.metric("실패", fail)

    if st.session_state.error_logs:
        with st.expander("⚠️ 작업 에러 로그", expanded=False):
            st.text_area(
                label="error_logs",
                value="\n".join(st.session_state.error_logs),
                height=220,
                disabled=True,
                label_visibility="collapsed",
            )

    if not st.session_state.combined_ddl:
        if total > 0 and fail > 0:
            st.error("생성 가능한 DDL 스크립트가 없습니다.")
        return

    file_name = f"GetOracleDDL_{datetime.now().strftime('%Y%m%d')}.sql"
    st.download_button(
        label=f"💾 Download Full DDL Script ({file_name})",
        data=st.session_state.combined_ddl,
        file_name=file_name,
        mime="text/plain",
    )

    tabs = st.tabs(
        ["요약", "SELECT COUNT", "DROP TABLE", "CREATE TABLE", "INDEX DROP/CREATE", "TABLE STATS"]
    )

    with tabs[0]:
        st.info(f"DDL 생성 완료: 성공 {success}건 / 실패 {fail}건")
    with tabs[1]:
        st.code(st.session_state.check_ddl_part or "", language="sql")
    with tabs[2]:
        st.code(st.session_state.drop_ddl_part or "", language="sql")
    with tabs[3]:
        if show_preview:
            st.code(st.session_state.body_ddl_part or "", language="sql")
        else:
            st.caption("CREATE 미리보기가 꺼져 있습니다. 다운로드 파일을 사용하세요.")
    with tabs[4]:
        index_file_name = f"GetOracleIndexDDL_{datetime.now().strftime('%Y%m%d')}.sql"
        st.download_button(
            label=f"Download Index DDL Script ({index_file_name})",
            data=st.session_state.index_ddl_part or "",
            file_name=index_file_name,
            mime="text/plain",
        )
        st.code(st.session_state.index_ddl_part or "", language="sql")
    with tabs[5]:
        stats_file_name = f"GatherTableStats_{datetime.now().strftime('%Y%m%d')}.sql"
        st.download_button(
            label=f"Download Table Stats Script ({stats_file_name})",
            data=st.session_state.stats_ddl_part or "",
            file_name=stats_file_name,
            mime="text/plain",
        )
        st.code(st.session_state.stats_ddl_part or "", language="sql")


def main() -> None:
    init_session_state()
    st.set_page_config(page_title="Oracle DDL Studio", layout="wide")
    render_styles()

    st.markdown(
        '<div class="main-banner"><div class="banner-title">ORACLE DDL STUDIO</div>'
        '<div class="banner-creator">⚙️ Created by ♡홍율파파♡</div></div>',
        unsafe_allow_html=True,
    )

    with st.form("ddl_form", clear_on_submit=False):
        try:
            login_owner, owner_options = get_owner_options()
            owner_index = owner_options.index(login_owner)
        except Exception:
            login_owner = ""
            owner_options = []
            owner_index = 0

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            if owner_options:
                selected_owner = st.selectbox(
                    "DDL Owner",
                    options=owner_options,
                    index=owner_index,
                    accept_new_options=True,
                )
            else:
                selected_owner = st.text_input("DDL Owner", value=login_owner)
        with c2:
            table_ts = st.text_input(
                "테이블 테이블스페이스",
                value=DEFAULT_TABLE_TS,
                placeholder="테이블스페이스명 입력",
            )
        with c3:
            index_ts = st.text_input(
                "인덱스 테이블스페이스",
                value=DEFAULT_INDEX_TS,
                placeholder="인덱스스페이스명 입력",
            )
        with c4:
            st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
            show_preview = st.checkbox("CREATE 미리보기", value=True)

        target_tables_input = st.text_area(
            "대상 테이블",
            value="",
            height=140,
            placeholder="테이블 명을 입력하세요 (쉼표 또는 공백 구분)\n예: ERP.ITEM_MST ERP.ITEM_DTL",
        )
        submitted = st.form_submit_button("DDL 생성", width="stretch")

    if submitted:
        try:
            run_generation(target_tables_input, selected_owner, table_ts, index_ts)
        except Exception as exc:
            st.session_state.error_logs = [f"[{datetime.now().strftime('%H:%M:%S')}] 시스템 오류 - {exc}"]
            st.session_state.last_total_count = 0
            st.session_state.last_success_count = 0
            st.session_state.last_fail_count = 1
            st.session_state.combined_ddl = None
            st.session_state.check_ddl_part = None
            st.session_state.drop_ddl_part = None
            st.session_state.body_ddl_part = None
            st.session_state.index_ddl_part = None
            st.session_state.stats_ddl_part = None

    render_result(show_preview)


if __name__ == "__main__":
    main()

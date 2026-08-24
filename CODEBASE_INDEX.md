# 코드베이스 인덱스

최종 갱신일: 2026-08-24

## 목적

이 저장소는 데이터베이스 운영과 마이그레이션을 위한 독립 실행형 Streamlit 도구 모음입니다. 각 `.py` 파일은 개별 앱의 진입점이며, 공용 애플리케이션 모듈은 현재 없습니다.

## 실행 방법

필요 패키지를 설치한 뒤, 대상 파일을 지정하여 실행합니다.

```powershell
py -m pip install -r requirements.txt
py -m streamlit run OracleDdlStudio.py
```

연결 정보는 PC별 `.streamlit/secrets.toml`에만 두며 Git에 올리지 않습니다.

## 앱 목록

| 파일 | 역할 | 주요 연결 설정 | 핵심 기능 |
| --- | --- | --- | --- |
| `OracleDdlStudio.py` | Oracle DDL 생성 | `[oracle]` | 선택한 소유자와 테이블의 구조, 인덱스, 주석, 권한, 통계 DDL을 단계별로 생성합니다. |
| `MssqlOracleLayoutDiff.py` | MSSQL-Oracle 레이아웃 비교 및 DDL | `[oracle]` | 스냅샷 레이아웃을 비교하고 Oracle 생성 DDL을 미리 보기·선택 실행합니다. 기존 세그먼트 할당량과 인덱스/제약 조건을 고려합니다. |
| `MssqlOracleCountCheck.py` | MSSQL-Oracle 건수·크기 검증 | `[mssql]`, `[oracle]` | `TB_MIG_TABLE_INFO`의 이관 대상을 읽어 양쪽 건수와 크기를 병렬로 비교하고 Excel 결과를 만듭니다. |
| `MssqlOracleServerMonitor.py` | MSSQL-Oracle 서버 상황판 | `[mssql]`, `[oracle]` | 여섯 MSSQL 소스 DB와 Oracle 타겟을 분리 표시합니다. Oracle은 지정 테이블스페이스·UNDO만 표시합니다. |
| `GreenplumRedshiftDailyLoad.py` | Greenplum 레이아웃 이력 적재·비교 | `[greenplum]`, `[redshift_sql]`, 선택 `[layout_history]` | 원천 스키마의 테이블/컬럼 레이아웃을 Redshift 이력 테이블에 적재하고 기준일 간 변경을 표시합니다. |
| `GreenplumRedshiftValidation.py` | Greenplum-Redshift 이관 검증 | `[greenplum]`, `[redshift_sql]`, 선택 `[migration_metadata]` | 메타데이터 또는 업로드 파일의 매핑을 기준으로 건수, 숫자 합계, PK 기반 행 해시를 병렬 검증하고 Excel 결과를 만듭니다. |
| `RedshiftSnapshotTableRestore.py` | Redshift 스냅샷 테이블 복구 | `[redshift]` 및 `redshift.targets.*` | 스냅샷에서 테이블을 별도 이름 또는 원본 대체 방식으로 복구합니다. 실행 이력, 상태 재조회, 의존 오브젝트 복구를 지원합니다. |

## 데이터 흐름

```text
MSSQL 6개 소스 서버 + Oracle 타겟 ── 서버 상황판
MSSQL 또는 Greenplum ── 레이아웃/데이터 검증 ── Oracle 또는 Redshift
Oracle ── DDL 생성 및 레이아웃 비교 ── 대상 Oracle 구조
Redshift 스냅샷 ── 테이블 복구 및 상태 모니터링 ── 대상 Redshift 데이터베이스
```

## 파일별 핵심 구조

### `OracleDdlStudio.py`

- `get_pool`, `get_owner_options`: Oracle 연결 풀과 선택 가능한 소유자를 준비합니다.
- `fetch_table_ddl`, `fetch_comment_block`, `fetch_grant_block`, `fetch_index_block`, `build_stats_block`: DBMS_METADATA와 데이터 사전에서 DDL 구성요소를 수집합니다.
- `generate_ddl_components`, `build_check_part`, `build_drop_part`: 생성·점검·삭제 단계의 결과를 조합합니다.
- `run_generation`, `render_result`, `main`: Streamlit 입력, 실행 상태, 결과 화면을 처리합니다.

### `MssqlOracleLayoutDiff.py`

- `fetch_layout_pair`, `compare_layouts`: 저장된 원천/대상 레이아웃을 읽고 차이를 계산합니다.
- `mssql_to_oracle`, `build_table_ddl`, `build_ddl_artifact`: MSSQL 타입을 Oracle 타입으로 변환하고 테이블 DDL을 생성합니다.
- `fetch_current_table_allocations`, `fetch_index_allocations`, `apply_storage_allocation`: 대상의 실제 저장공간 할당량을 DDL에 반영합니다.
- `fetch_table_catalog_metrics`, `render_ddl_controls`, `execute_ddl`: 카탈로그 지표 표시와 선택 DDL 실행을 담당합니다.

### `MssqlOracleCountCheck.py`

- `fetch_metadata_mappings`, `load_excel_mappings`: `TB_MIG_TABLE_INFO` 또는 Excel에서 비교 대상을 읽습니다.
- `mssql_identifier`, `oracle_identifier`: 데이터베이스 식별자를 안전하게 구성합니다.
- `fetch_mssql_measurement`, `fetch_oracle_count`: 양쪽 건수와 MSSQL 크기 정보를 조회합니다.
- `run_comparison`: 작업별 상태와 시간을 보존하면서 병렬 비교를 실행합니다.
- `result_excel_bytes`, `render_results`: 화면과 서식화된 Excel 결과를 생성합니다.

### `MssqlOracleServerMonitor.py`

- `mssql_config`, `monitor_mssql_databases`, `monitor_oracle_server`: 기존 MSSQL 연결 설정의 기본 로그인 DB로 한 번 로그인한 뒤, 여섯 소스 DB의 `sys.databases.state_desc`와 실제 DB 조회 오류 원문을 매회 표시합니다. Oracle 조회 성공도 MSSQL과 같은 `ONLINE`으로 표시하며, 특정 소스 DB를 연결 기준으로 고정하거나 상태 원인을 해석하지 않습니다.
- `load_snapshot`, `refresh_snapshot`: MSSQL 인증 실패 시 자동 재조회를 중지합니다. 브라우저 F5로 새 화면 세션을 열면 다시 조회하며, 조회 성공 시 DB별·Oracle 사용자별 세션 추이를 누적합니다.
- `render_live_board`, `render_situation_board`, `situation_board_payload`: 지속형 상황판 컴포넌트에서 카드 틀을 유지한 채 조회값만 갱신합니다. 각 카드 우측 상단에는 해당 DB 서버가 반환한 `YYYY-MM-DD HH24:MI:SS` 시각을 표시합니다. MSSQL은 2열 3행 카드에 DB별 세션 추이를, Oracle은 `PCERP_RENTALAPP`과 `PCERP_RENTALAPP_MIG`의 세션 추이를 각각 표시합니다. Oracle 카드는 두 세션 행과 세 자리 세션값을 위한 210px 최소 높이와 표시 공간을 확보합니다. 카드명은 상태값보다 크게 표시하고 Oracle 카드명은 `Oracle`로 표시합니다. 모든 카드의 DATA GB와 Oracle 테이블스페이스 표의 전체 GB·사용 GB는 GB와 TB를 함께 표시하며 표는 지정 테이블스페이스·UNDO만 표시합니다.

### `GreenplumRedshiftDailyLoad.py`

- `fetch_snapshot_dates`, `fetch_layout_pair`, `compare_layouts`: 적재 이력에서 기준일별 레이아웃 차이를 조회합니다.
- `list_source_schemas`, `fetch_source_layout`, `save_layout`: Greenplum 원천 메타데이터를 수집해 Redshift 이력 테이블에 저장합니다.
- `main`: 조회 기준일 선택, 적재, 변경 내역 표시를 구성합니다.

### `GreenplumRedshiftValidation.py`

- `fetch_metadata`, `load_excel`, `normalize_mappings`: 검증 대상 매핑을 수집·정규화합니다.
- `metrics`, `primary_key_columns`, `hash_rows`, `hash_mismatch_rows`: 건수·합계·PK 행 해시 검증 데이터를 계산합니다.
- `validate`, `run_comparison`: 테이블 단위 검증을 병렬 실행하고 실패를 개별 결과로 유지합니다.
- `excel_bytes`, `render_results`: 결과, 상세 차이, 해시 불일치를 Excel로 제공합니다.

### `RedshiftSnapshotTableRestore.py`

- `list_snapshots`, `validate_snapshot`, `build_records`: 복구 가능한 스냅샷과 대상 테이블을 검증합니다.
- `execute_restore_worker`, `start_restore_worker`, `wait_for_restore`: 장시간 복구를 별도 작업으로 실행하고 상태를 대기합니다.
- `capture_recovery_artifacts`, `drop_target_table`, `recreate_dependent_objects`: 원본 대체 복구 시 의존 오브젝트와 권한을 보존·복구합니다.
- `save_run`, `load_run`, `refresh_run_statuses`: `redshift_restore_runs`에 실행 이력을 저장하고 재조회합니다.
- `live_restore_fragment`: 진행 상태를 주기적으로 화면에 갱신합니다.

## 의존성 및 로컬 산출물

- `requirements.txt`: Streamlit, Oracle, AWS, 데이터프레임, MSSQL, Excel, PostgreSQL 드라이버 의존성을 정의합니다.
- `.streamlit/config.toml`: 공유 가능한 화면 테마 설정입니다.
- `.streamlit/secrets.toml`: 연결 비밀값이며 Git 제외 대상입니다. 상황판도 기존 `[mssql]`, `[oracle]` 설정만 사용합니다.
- `PC_REBUILD_GUIDE.md`: PC 포맷 후 Python·Git·드라이버·AWS 프로필 설치, 소스 복제, 로컬 비밀 설정, 앱 실행과 점검 순서를 안내합니다.
- `redshift_restore_runs/`, `.cache/`, `tmp/`, `__pycache__/`: 로컬 실행 산출물이며 Git 제외 대상입니다.
- `packages/`: 로컬 Python wheel 보관 폴더이며 현재 Git 미추적 상태입니다.

## 변경 시 갱신 기준

다음이 바뀌면 이 문서를 같은 작업에서 갱신합니다.

- `.py` 파일 추가·삭제·이름 변경 또는 핵심 기능 변경
- 데이터베이스 연결 방식, 비밀 설정 섹션, 메타데이터 테이블 변경
- 실행 방법, 의존성, 로컬 산출물, Git 제외 규칙 변경
- 새로운 SQL/DDL 파일 또는 SQL 폴더 추가

코드 변경 후에는 관련 파일의 문서 설명과 실행·설정 정보를 실제 코드에 맞춰 함께 검토합니다.

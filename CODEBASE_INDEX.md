# 코드베이스 인덱스

최종 갱신일: 2026-08-25

## 문서 기준

| 문서 | 책임 |
| --- | --- |
| `CODEBASE_INDEX.md` | 저장소 앱·폴더·실행 진입점 인덱스 |
| `10.Gp2Red/README.md` | 이관 관리 메뉴, 최초 투입, 접속·권한·매핑, 검증 기준 |
| `PC_REBUILD_GUIDE.md` | 새 Windows PC의 개발 환경·비밀 설정·이관 메타 복원 절차 |

연결 비밀값은 PC별 `.streamlit/secrets.toml`에만 관리하며 Git과 문서에 기록하지 않습니다.

## 실행 진입점

| 파일 | 용도 | 주요 연결 설정 |
| --- | --- | --- |
| `OracleDdlStudio.py` | Oracle DDL 생성 | `[oracle]` |
| `MssqlOracleLayoutDiff.py` | MSSQL-Oracle 레이아웃 비교·DDL 및 컬럼 매핑·변경 사유 Excel | `[mssql]`, `[oracle]` |
| `MssqlOracleCountCheck.py` | MSSQL-Oracle 건수·크기 검증 | `[mssql]`, `[oracle]` |
| `MssqlOracleServerMonitor.py` | MSSQL-Oracle 서버 상황판 | `[mssql]`, `[oracle]` |
| `RedshiftSnapshotTableRestore.py` | Redshift 스냅샷 테이블 복구 | `[redshift]`, `redshift.targets.*` |
| `10.Gp2Red/app/SrcTgtOrchestrator.py` | 로컬 로그인 기반 원천·대상 이관 관리와 구조조회 | `[migration_metadata]`, 접속관리의 Secrets 섹션명 |
| `10.Gp2Red/app/SrcTgtMonitor.py` | 고객 제공 이관 현황 | `[migration_monitor]` 또는 `[migration_metadata]` |

`MssqlOracleCountCheck.py`와 `MssqlOracleServerMonitor.py`의 MSSQL 원천 목록은 `CERDB`, `HS_RESORT`, `INSIDEBANK`, `JBNDB`, `MEMDB`, `PREEDDB`, `SALDB`입니다.

## 폴더

```text
10.Gp2Red/
├─ app/        Streamlit 화면과 이관 관리 모듈
├─ dag/        화면에서 생성한 실제 Airflow DAG와 공통 실행 래퍼
├─ sql/        신규 프로젝트 초기화 DDL과 기존 환경 정리 SQL
├─ tests/      비밀값 없는 이관 가상 흐름 검증
├─ log/        DAG 파일 로그
├─ artifact/   생성한 Excel 산출물
└─ README.md   기능·운영 단일 기준 문서
99.back/       통합 전 원본 보관
packages/      오프라인 설치용 wheel 보관
```

`artifact`, `log`, `redshift_restore_runs`, `.cache`, `tmp`, `__pycache__`, `packages`는 로컬 실행·배포 산출물입니다. 이관 프로젝트의 상세 구조와 생성 규칙은 [10.Gp2Red/README.md](10.Gp2Red/README.md)를 기준으로 합니다.

`10.Gp2Red/app/SrcTgtOrchestrator.py`는 메타가 준비되지 않은 PC에서는 `SrcTgtSetup.py` 초기 설정 화면을 먼저 표시합니다. `.streamlit/migration_setup.toml`은 선택한 스키마명만 보관하는 로컬 Git 제외 파일입니다.

`10.Gp2Red/app/SrcTgtSetup.py`는 DBA가 생성한 스키마를 확인한 뒤 그 안의 메타를 초기화하며, 기존 메타는 `테이블명_YYYYMMDD` CTAS 백업 후 다시 만들 수 있습니다. 스키마 생성·삭제는 수행하지 않고 선택값은 Git 제외 로컬 설정에 보관합니다. `10.Gp2Red/sql/01_mig_metadata_ddl.sql`은 초기 화면에서 입력한 스키마명으로 치환되어 이관 전용 메타 뷰·테이블을 `DROP IF EXISTS → CREATE` 합니다. `10.Gp2Red/app/SrcTgtConnection.py`는 원천·대상 접속 ID, DBMS, Secrets 섹션명, Airflow 접속 ID와 사용 여부를 관리하며 실제 비밀번호를 저장하거나 표시하지 않습니다. `10.Gp2Red/app/SrcTgtLayoutHistory.py`는 원천 접속 ID별 레이아웃 수집·기준일 비교를 제공하며, 기존 이력 테이블에는 최초 조회 시 `SRC_CONN_ID` 컬럼을 추가합니다. `SrcTgtTargetReflection.py`는 원천 기준일과 대상 매핑을 비교해 물리설계·DDL을 관리합니다. `10.Gp2Red/tests/virtual_workflow_test.py`는 초기 스키마·메타 백업 명명 규칙·메타 초기화 DDL·접속 마스터·비밀번호·사용자 그룹·원천 레이아웃 교체·원천→대상 자동 매핑·대상 반영안·DAG·가상 EXTRACT/LOAD를 실제 연결 없이 검증합니다. `10.Gp2Red/app/SrcTgtUser.py`는 `관리자` 전용 사용자 등록·비밀번호 초기화·권한 범위 관리 모듈입니다. 비밀번호는 `TB_MIG_USR`에 scrypt 단방향 해시로만 저장합니다. `10.Gp2Red/app/SrcTgtMapping.py`는 적재한 원천 접속별 레이아웃을 선택해 원천 컬럼을 불러오고, 접속관리에서 등록한 원천·대상 접속 ID만 매핑에 사용합니다. 대상 구조 자동 반영은 선택한 대상 접속의 Secrets 설정을 사용합니다. `10.Gp2Red/app/SrcTgtControl.py`는 상위 주제영역명을 코드로 연결해 표시하고, 주제영역 코드 변경 시 연결된 하위 주제영역·테이블 매핑·사용자 권한·DAG 메타를 함께 갱신합니다. 관리자 메뉴는 사용자와 접속정보를 하나로 관리합니다. 실행 주제영역의 사용 여부가 DAG 생성·실행 대상 여부를 함께 제어합니다. `SrcTgtDagGenerator.py`는 병렬도 범위와 사용 중인 DAG 생성 대상을 사전 검증하고, 접속 마스터에서 해석한 Airflow 접속 ID를 실행 레코드에 포함합니다. `10.Gp2Red/dag/common/mig_step_runtime.py`는 실행기 호출 전후 `EXTRACT`, `LOAD`의 상태와 시작·종료·경과시간을 기록하는 DAG 공통 래퍼입니다.

## 접속 메타 보완 DDL

기존 메타를 유지하면서 접속 마스터만 추가할 때는 `10.Gp2Red/sql/02_mig_connection_migration.sql`을 사용합니다. 이 파일은 기존 테이블·사용자·매핑·실행 이력을 삭제하지 않습니다.

## 기본 실행

```powershell
py -m pip install -r requirements.txt
py -m streamlit run 10.Gp2Red\app\SrcTgtOrchestrator.py
py -m streamlit run 10.Gp2Red\app\SrcTgtMonitor.py
python 10.Gp2Red\tests\virtual_workflow_test.py -v
```

각 도구는 독립 실행형입니다. 같은 PC에서 동시에 실행할 때는 포트를 다르게 지정합니다.

## 변경 시 갱신 기준

- Python, SQL, 설정, 의존성, 실행 방법을 바꾸면 이 문서를 함께 갱신합니다.
- 이관 관리의 기능·권한·메타데이터·운영 절차를 바꾸면 `10.Gp2Red/README.md`를 갱신합니다.
- 새 파일 추가·삭제·이름 변경 시 실행 진입점과 폴더 목록을 함께 갱신합니다.

# PC 포맷 후 개발 환경 복원

최종 확인일: 2026-08-25

이 문서는 새 Windows PC에서 이 저장소의 Streamlit 도구를 다시 실행하기 위한 절차입니다. 도구 기능과 운영 절차는 [CODEBASE_INDEX.md](CODEBASE_INDEX.md), 이관 관리는 [10.Gp2Red/README.md](10.Gp2Red/README.md)를 기준으로 합니다.

## 1. 포맷 전 보관

사내 승인된 암호 관리 도구 또는 암호화된 보관소에만 보관합니다.

- `.streamlit\secrets.toml`의 실제 연결 정보
- Redshift AWS 프로필과 IAM 접근 권한
- 필요한 ODBC 드라이버 설치 파일 또는 설치 경로
- 필요 시 `redshift_restore_runs\` 복구 이력

`.streamlit\secrets.toml`과 비밀번호, 접속 문자열, AWS 키는 Git·문서·채팅에 넣지 않습니다.

## 2. 프로그램 설치

1. Python 3.14 64비트
2. Git for Windows
3. Microsoft ODBC Driver for SQL Server
4. Redshift 스냅샷 복구 사용 시 AWS CLI

```powershell
py --version
git --version
```

Oracle 도구는 Python `oracledb`를 사용합니다. Oracle Client는 연결 오류로 요구될 때만 설치합니다.

## 3. 소스와 가상 환경

```powershell
Set-Location C:\Users\<Windows사용자>\Documents
git clone https://github.com/lovehamtory/00.StreamlitCode.git 00.code
Set-Location C:\Users\<Windows사용자>\Documents\00.code
py -3.14 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip check
```

PowerShell 실행 정책으로 활성화가 막히면 현재 창에서만 다음을 실행합니다.

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\.venv\Scripts\Activate.ps1
```

## 4. PC별 비밀 설정

```powershell
New-Item -ItemType Directory -Force .streamlit
notepad .streamlit\secrets.toml
git check-ignore -v .streamlit\secrets.toml
```

| 도구 | 설정 섹션 |
| --- | --- |
| Oracle DDL, MSSQL-Oracle 레이아웃·건수·상황판 | `[oracle]`, 필요 시 `[mssql]` |
| 원천·대상 이관 관리 | `[migration_metadata]`, 메타 저장소용 `[redshift_sql]`, 접속관리에서 등록한 원천·대상별 Secrets 섹션, 선택 `[migration_monitor]`, `[layout_history]` |
| Redshift 스냅샷 복구 | `[redshift]`, `redshift.targets.<이름>` |

원천·대상 이관의 DB 연결 섹션에는 `host`, `port`, `database`, `user`, `password`가 필요합니다. `[migration_metadata]`는 메타데이터 연결 섹션과 스키마를 지정하며, 고객 현황용 `[migration_monitor]`는 조회 전용 연결 섹션을 지정합니다. 프로그램 초기화 뒤 `👤 관리 > 접속`에서 각 원천·대상 접속의 Secrets 섹션명과 Airflow 접속 ID를 등록합니다. 실제 비밀번호는 메타 테이블에 입력하지 않습니다.

기존 이관 메타를 유지하는 PC는 초기화 화면을 다시 실행하지 말고 `10.Gp2Red\sql\02_mig_connection_migration.sql`을 메타 스키마에 한 번 적용합니다. 스키마명이 `MIG_META`가 아니면 실행 전에 스키마명만 치환합니다.

MSSQL ODBC 드라이버 이름은 다음으로 확인합니다.

```powershell
python -c "import pyodbc; print(pyodbc.drivers())"
```

## 5. 점검과 실행

```powershell
python -m py_compile OracleDdlStudio.py MssqlOracleLayoutDiff.py MssqlOracleCountCheck.py MssqlOracleServerMonitor.py RedshiftSnapshotTableRestore.py
Get-ChildItem 10.Gp2Red\app -Filter *.py | ForEach-Object { python -m py_compile $_.FullName }
python 10.Gp2Red\tests\virtual_workflow_test.py -v
python -m streamlit run 10.Gp2Red\app\SrcTgtOrchestrator.py
python -m streamlit run 10.Gp2Red\app\SrcTgtMonitor.py --server.port 8502
```

`py_compile`은 문법만 검사합니다. 가상 테스트는 접속정보 없이 메타·접속관리·레이아웃·DAG·로그 흐름을 확인합니다. 로컬 사용자 로그인, DB 연결, Airflow, S3, 실제 이관은 운영 접근 권한으로 별도 확인해야 합니다.

## 6. 일상 동기화와 장애 확인

```powershell
Set-Location C:\Users\<Windows사용자>\Documents\00.code
git pull
```

`git pull`은 소스만 갱신합니다. 비밀 설정, AWS 프로필, 실행 이력은 각 PC에 남습니다. 오류 시에는 가상 환경 활성화, `python -m pip check`, 설정 섹션 존재 여부, ODBC 드라이버, AWS 프로필·IAM 권한, 포트 충돌 순서로 확인합니다.

# SRC → S3 → TGT 이관 관리 운영자 매뉴얼

이 도구는 Greenplum 등 원천 DB에서 S3 Parquet 기준본을 만들고, 검증된 기준본을 Redshift 등 대상으로 이행하기 위한 이관 전용 관리 도구입니다. Streamlit은 메타 관리·DAG 생성·Airflow 배포·로그 조회·산출물 생성을 담당합니다. 실제 이관 실행은 Airflow가 담당합니다.

비밀번호, 접속문자열, AWS 키는 화면·메타 테이블·Git에 저장하지 않습니다. PC별 `.streamlit/secrets.toml`과 Airflow Connection/Variable에만 관리합니다.

## 1. 업무 구조

```text
접속정보 ── 원천 레이아웃 ── SRC·TGT 테이블/컬럼 매핑 ── 대상 DDL
                                                   │
                                      주제영역별 전체 DAG 생성
                                                   │
                                 테이블별 증분·일회성 DAG 생성
                                                   │
                           Airflow 실행 ── 실행로그·검증결과 ── 산출물
```

- 주제영역은 `A01`, `A010001`처럼 등록하며 DAG 분할 단위입니다.
- 전체 이관은 주제영역별로 `원천→S3`, `S3→대상` DAG를 각각 생성합니다.
- 증분은 테이블별로 `원천→S3`, `S3→대상`, 두 단계를 연결한 통합 DAG를 각각 생성합니다.
- 검증은 `SRC→S3`, `S3→TGT` 두 구간으로 실행하며 COUNT는 필수, SUM/HASH는 컬럼 매핑의 Y/N으로 처리합니다.
- S3는 FULL·INCR을 분리한 Parquet 기준본이며, 대상 이행은 실행회차의 검증 완료 매니페스트만 사용합니다.

## 2. 메뉴와 사용 순서

| 순서 | 메뉴 | 작업 | 결과 |
| --- | --- | --- | --- |
| 1 | 초기 설정 | 메타 스키마 백업·생성 | 이관 메타 테이블 생성 |
| 2 | 이관 관리 > 접속정보 | 원천·대상 접속 식별자, DBMS, S3 기준경로 등록 | 매핑 선택 목록 준비 |
| 3 | 이관 관리 > 주제영역 | 상위·하위 주제영역, 원천·대상 접속정보, 사용여부 등록 | DAG 분할·접속 방향 준비 |
| 4 | 구조·변경 > 원천 레이아웃 | 원천 기준일·스키마 수집 | 컬럼·PK·NULL 기준본 저장 |
| 5 | 이관 관리 > SRC·TGT 매핑 | 테이블·컬럼, 표준 대상명, 변환식, 검증 규칙 등록 | 이행 규칙 확정 |
| 6 | 구조·변경 > 대상 반영안 | 대상 DDL 조회·수정·저장·적용 | DROP·CREATE 및 COMMENT ON 실행 |
| 7 | 이관 관리 > Airflow | Airflow 환경과 배포 방식을 등록 | 자동 배포 대상 준비 |
| 8 | 이관 관리 > EMR | 전용·공용 EMR과 자동 종료 정책 등록 | 비용 보호 정책 준비 |
| 9 | 이관 관리 > DAG 생성 | 주제영역 전체 또는 테이블별 DAG 생성·배포 | 로컬 DAG 보관, Airflow 비활성 등록 |
| 10 | Airflow | Connection/Variable 설정, 실행 순서 결정 | 실제 이관 실행 |
| 9 | 실행 현황·검증 | 상태·건수·오류·검증 결과 확인 | 보정 대상 식별 |
| 10 | 산출물 | 정의서·테스트·검증 결과서 생성 | Excel 파일 생성 |

## 3. 설치와 화면 실행

### 3.1 Python 설치 확인

1. Python 3.12 이상을 설치합니다.
2. 설치 첫 화면에서 `Add python.exe to PATH`를 선택합니다.
3. PowerShell을 새로 열고 아래를 실행합니다.

```powershell
py --version
```

### 3.2 프로젝트 준비

PowerShell에서 한 줄씩 실행합니다.

```powershell
cd $env:USERPROFILE\Documents
git clone https://github.com/lovehamtory/00.StreamlitCode.git
cd .\00.StreamlitCode
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m streamlit run 10.Gp2Red\app\SrcTgtOrchestrator.py
```

`스크립트를 실행할 수 없습니다`가 표시되면 현재 PowerShell 창에서 아래를 실행한 뒤 다시 활성화합니다.

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

| 라이브러리 | 용도 |
| --- | --- |
| `streamlit`, `pandas` | 관리 화면과 목록 처리 |
| `psycopg[binary]` | Greenplum·Redshift 계열 접속 |
| `oracledb` | Oracle 접속 확장 시 사용 |
| `pyodbc` | MSSQL 접속 확장 시 사용 |
| `openpyxl` | Excel 산출물 생성 |
| `boto3` | Redshift 스냅샷 복구 |

종료는 실행한 PowerShell에서 `Ctrl + C`입니다.

## 4. 초기 설정

`초기 설정` 메뉴에서 DBA가 생성한 메타 스키마명을 입력합니다. 먼저 `메타데이터 백업`을 누르면 기존 이관 메타 테이블만 날짜 접미어 CTAS 백업으로 보관합니다. 다음으로 `메타 생성`을 누르면 `sql/01_mig_metadata_ddl.sql`을 그 스키마명으로 치환해 실행합니다.

메타 생성은 이관 메타 테이블·뷰를 재생성합니다. 기존 운영 메타가 있으면 반드시 백업 후 실행합니다. 대상 업무 테이블·원천 테이블·Redshift 물리 테이블을 생성하거나 변경하지 않습니다.

## 5. 접속정보

접속정보는 방향을 갖지 않습니다. 한 접속이 어떤 매핑에서는 원천이고 다른 매핑에서는 대상이 될 수 있으므로, 방향은 주제영역에서 정의합니다.

| 항목 | 허용값 | 용도 |
| --- | --- | --- |
| 접속 ID | 영문 시작, 영문·숫자·밑줄 | 매핑 식별자 및 Airflow Connection 이름 |
| DBMS | `GREENPLUM`, `REDSHIFT`, `ORACLE`, `MSSQL`, `POSTGRESQL`, `OTHER` | 접속 엔진 식별 |
| 문자길이배수 | `1`, `2`, `3`, `4` | 원천 문자형 → VARCHAR 자동 변환 배수 |
| S3 기준경로 | `s3://`로 시작 또는 공란 | 대상 접속에만 입력 |
| Secrets 섹션명 | PC별 Secrets 섹션명 | 실제 접속값 참조 |
| 사용 | 사용/미사용 | 미사용 접속은 신규 매핑 선택 불가 |

대상 접속의 S3 기준경로 아래에 FULL·INCR 기준본을 생성합니다.

## 6. 주제영역

상위주제영역과 주제영역을 계층으로 관리합니다. 상위 `A01`을 먼저 등록하고, 그 아래에 `A010001`~`A010010`을 등록합니다. 접속정보와 DAG 분할은 하위 주제영역에만 지정하며, 테이블 매핑은 하위 주제영역에 연결합니다.

| 항목 | 설명 |
| --- | --- |
| 상위주제영역코드·명 | `A01` 및 업무 분류명 |
| 주제영역코드·명 | `A010001` 및 세부 업무명. 상위주제영역 필수 |
| 원천접속ID | 하위 주제영역의 SRC 연결 |
| 대상접속ID | 하위 주제영역의 TGT 연결 및 S3 기준경로 |
| 표시순서 | 화면 정렬용 |
| 사용 | 사용 중인 주제영역만 매핑·DAG 생성 대상 |

주제영역 간 선후행과 전체 오케스트레이터는 만들지 않습니다. 전체 실행의 병렬·순차 순서는 Airflow에서 프로젝트 상황에 맞게 구성합니다.

## 7. 원천 구조·변경과 대상 반영안

`구조·변경` 화면은 매일 실행하는 배치가 아닙니다. 신규 매핑, 원천 레이아웃 변경, DDL 검토가 필요할 때만 사용합니다.

| 업무 | 입력 | 결과 |
| --- | --- | --- |
| 원천 레이아웃 | Greenplum 접속, 기준일, 스키마 | 원천 테이블·컬럼·PK·NULL 이력 |
| 변경 비교 | 이전·비교 기준일 | 신규·삭제·변경 컬럼 목록 |
| 대상 반영안 | 저장된 매핑과 대상 접속 | 기존 DDL 조회, 물리 옵션 수정, DDL 적용 |

원천 수집은 현재 Greenplum 카탈로그를 사용합니다. Oracle·MSSQL 원천은 동일한 `TB_MIG_SRC_LAYOUT` 형식으로 업로드 또는 수집 모듈을 추가한 뒤 사용합니다.

타입 변환의 기본 규칙은 문자형 `VARCHAR(원천길이 × 접속별 배수)`, 일반 수치형 `DECIMAL`, 날짜·시간형 `TIMESTAMP`입니다. Redshift가 동일 타입을 지원하는 BOOLEAN·BIT 등은 유지합니다. 한글 원천명과 표준 영문 대상명은 분리 관리하며, 실행 SQL은 테이블 단위로 이관·이행을 독립 저장합니다.

## 8. SRC·TGT 매핑

테이블과 컬럼은 같은 화면에서 등록·수정합니다. 신규 매핑은 주제영역을 먼저 선택하고 수집한 원천 레이아웃에서 시작합니다. 원천·대상 접속ID는 주제영역에서 읽기 전용으로 표시됩니다. 필요하면 대상 구조 자동 반영을 사용합니다. 일괄 업로드는 테이블매핑·컬럼매핑 두 시트를 한 Excel 파일로 입력합니다.

### 테이블 항목 코드값

| 항목 | 코드값 | 설명 |
| --- | --- | --- |
| 적재 상태 | `FULL`, `INCR` | 기본 전체·증분 운영 구분 |
| 시스템 컬럼명 | 쉼표·공백 또는 JSON 배열 | 생성일시·수정일시 등 원천 추출 기준 컬럼. 배열의 컬럼은 `OR` 조건 |
| 시스템 컬럼 형식 | `YYYYMMDD`, `YYYYMMDDHH24MISS`, `TIMESTAMP`, `DATE` | 증분 기준값 변환 형식 |
| 증분 방식 | `PK_MERGE`, `APPEND` | PK 또는 증분컬럼 기준 `DELETE·INSERT` |
| 원천 증분 컬럼명 | 쉼표·공백 또는 JSON 배열 | 원천 PK가 있으면 신규 매핑 시 자동 채움. 대상 컬럼은 컬럼매핑으로 자동 변환 |
| S3 병렬 방식 | `NONE`, `WHERE` | 단일 추출 또는 WHERE 조건별 병렬 추출 |

시스템 컬럼명과 증분 컬럼명은 `생성일시, 수정일시`처럼 쉼표·공백으로 입력하거나 JSON 배열로 입력합니다. 저장 후에는 저장된 배열값을 다시 표시합니다.

```text
["생성일시", "수정일시"]
["PK1", "PK2"]
```

### 컬럼 매핑 방식

컬럼매핑은 대상 컬럼을 먼저 정의합니다. `TB_MIG_COL_MPG`는 `MPG_ID`로 테이블매핑과 연결하며, 컬럼매핑 업로드에 `MPG_ID`가 없으면 원천·대상 테이블 식별값으로 해당 테이블매핑을 찾습니다.

| 방식 | 이행 적용SQL식 | 원천 참조컬럼명 |
| --- | --- | --- |
| `MOVE` | 선택 | 원천컬럼명 |
| `CONST` | 필수. 예: `'Y'`, `0` | 입력하지 않음 |
| `NULL` | 입력하지 않음 | 입력하지 않음 |
| `EXPR` | 필수 | 산식·조인에 참조한 원천 컬럼 |

컬럼매핑은 대상 레이아웃과 산출물의 기준입니다. 이관 SQL은 원천 레이아웃을 S3에 적재하는 테이블 단위 SQL이며, 원천 함수·조인·복호화가 필요하면 이관 SQL에서 직접 작성합니다. 이행 SQL은 S3 스테이지와 대상 테이블·기존 테이블을 조인할 수 있는 테이블 단위 SQL입니다.

```text
["CUST_NO", "CUST_NM"]
```

룩업, 시퀀스, 조건분기, 마스킹, 형변환은 별도 방식으로 늘리지 않고 `EXPR`의 SQL식으로 처리합니다. 대상 테이블의 DDL 기본값은 컬럼매핑방식이 아니며, `DFLT_EXPR`은 MOVE·산식 결과가 NULL일 때 적용할 기본값 SQL식입니다.

### SQL 생성·수정

매핑 그리드의 `SQL 생성`은 원천 컬럼을 기준으로 SRC→S3 이관 SQL과 대상 컬럼매핑을 기준으로 S3→TGT 이행 SQL을 각각 테이블 단위로 생성해 저장합니다. 기존 SQL은 이력에 남으므로 언제든 복원할 수 있습니다. SQL 탭에서는 두 SQL을 독립적으로 수정한 뒤 `SQL 저장`으로 반영합니다.

생성된 DAG는 SQL 본문을 파일에 고정하지 않고 실행할 때마다 `TB_MIG_TBL_MPG.SRC_EXT_SQL`, `TB_MIG_TBL_MPG.TGT_LOAD_SQL`을 조회합니다. 따라서 수정 SQL을 저장하면 해당 DAG 파일을 다시 생성하지 않아도 다음 실행부터 수정 SQL을 사용합니다.

수정 SQL은 `TB_MIG_MPG_CHG_HIST`에 이전 SQL과 변경 SQL을 함께 저장합니다. `SQL 이력` 탭에서 원하는 이력을 선택해 복원할 수 있으며, 복원 동작도 새로운 SQL 이력으로 남습니다.

| SQL | 필수 치환값 | 용도 |
| --- | --- | --- |
| 원천 추출 SQL | `__SRC_WHERE_CND__` | 증분 기준·수동 재작업 조건·병렬 조건 반영 |
| 대상 적재 SQL | `__MIG_STAGE__` | 원천 레이아웃 S3 스테이지 참조 |
| 대상 적재 SQL | `__TGT_TABLE__` | 현재 대상 스키마·테이블명으로 자동 치환 |

이관 SQL에는 조인·룩업·원천 함수·복호화 등을, 이행 SQL에는 ETL 적재일시·대상 기본값·Redshift 전용 식과 대상 테이블 조인을 작성할 수 있습니다. 저장 시 이관 SQL은 단일 SELECT와 명시 컬럼을, 이행 SQL은 INSERT 대상·SELECT 컬럼수와 컬럼매핑수를 검증합니다. `*`는 허용하지 않습니다.

생성 DAG는 시스템 컬럼 배열을 `OR`로 연결해 영향을 받은 원천 증분키를 찾습니다. 예를 들어 `STD_DT IN (SELECT STD_DT ... WHERE 생성일시 >= 기준값 OR 변경일시 >= 기준값)`을 생성합니다. S3에는 이관 SQL의 원천 레이아웃을 적재하고, 대상 적재는 이행 SQL과 대상 컬럼 매핑을 사용합니다. `FULL`은 `TRUNCATE TABLE` 후 `INSERT`합니다. `PK_MERGE`와 `APPEND`는 모두 컬럼매핑으로 변환된 대상 증분키 범위를 `DELETE`한 뒤 `INSERT`합니다. SQL `MERGE` 문은 생성하지 않습니다.

### S3 기준본 경로와 보관

S3 경로는 대상 접속의 S3 기준경로 아래에 아래처럼 생성합니다. S3는 실제 폴더가 아닌 접두어입니다.

```text
s3://기준경로/
  full/{대상스키마}__{대상테이블}/...
  incr/{대상스키마}__{대상테이블}/wrk_dt=YYYYMMDD/run_id={DAG실행ID}/...
```

- FULL 실행은 `full/{대상스키마}__{대상테이블}/`를 비운 뒤 새 기준본을 생성합니다. 일회성 재적재도 FULL 경로를 사용합니다.
- INCR 실행은 실행일·실행회차별 파일을 추가하고, 해당 테이블의 증분 기준본은 최근 31일만 보관합니다.
- `run_s3` 실행기는 DAG가 넘기는 `s3_load_path`, `s3_cleanup_prefix`, `s3_cleanup_before_write`, `s3_retention_days`를 적용합니다. 대상 이행은 경로를 추측하지 않고 `TB_MIG_S3_MANF`의 해당 실행회차 파일만 읽습니다.

`WHERE` 병렬 조건은 테이블 단위 JSON 배열입니다. 조건 하나가 S3 추출 작업 하나가 되며 대상 적재는 테이블별 단일 실행입니다.

```text
["abc_dt BETWEEN '19000101' AND '20001231'", "abc_dt BETWEEN '20010101' AND '20261231'"]
```

컬럼 매핑의 `SUM_VALD_YN`, `HSH_VALD_YN`만 선택값입니다. COUNT 검증은 항상 수행합니다. 매핑 저장 시 메타 버전이 증가하고 `TB_MIG_MPG_CHG_HIST`에 테이블·컬럼 변경 이력이 남습니다.

## 9. Airflow·EMR 관리와 DAG 자동 배포

### 9.1 Airflow

Airflow는 여러 환경을 등록할 수 있습니다. 실제 인증정보는 `TB_MIG_AIRFLOW`가 아닌 PC별 Secrets에만 둡니다. 배포 방식은 아래 둘 중 하나를 선택합니다.

| 배포방식 | Secrets 필수 항목 | 동작 |
| --- | --- | --- |
| `SHARED_PATH` | `dag_deploy_root`, `airflow_api_url` | Streamlit 서버가 공유 DAG 경로에 원자적으로 파일 저장 후 Airflow API로 비활성 전환 |
| `DEPLOY_AGENT` | `deploy_agent_url`, `deploy_agent_token`, `airflow_api_url` | 배포 에이전트에 파일 전달 후 Airflow API로 비활성 전환 |

Airflow API는 DAG 파일을 업로드하는 기능이 아닙니다. 따라서 공유 경로 또는 별도 배포 에이전트가 필수입니다. Airflow API 인증은 `airflow_api_token` 또는 `airflow_api_username`·`airflow_api_password`를 Secrets에 둡니다. 파일 저장 후 Airflow가 DAG를 인식할 때까지 최대 30초 동안 확인하고, 인식된 DAG만 paused 상태로 전환합니다. 생성 실패·배포 실패·비활성 전환 실패는 `TB_MIG_DAG_DPLY_HIST`에 남습니다.

### 9.2 EMR

EMR은 `EMR_EC2`, `EMR_SERVERLESS`를 등록할 수 있습니다. `전용 EMR`일 때만 `DAG 종료 후 자동 종료`를 선택할 수 있습니다. 공용 EMR은 다른 업무가 사용할 수 있으므로 자동·화면 강제 종료를 허용하지 않습니다.

EMR 자동 종료는 S3 또는 INS 단독 DAG에는 붙지 않고, `ALL` DAG가 성공·실패로 종료된 뒤에만 실행됩니다. 종료 요청도 실패하면 DAG 실패로 기록되며 `TB_MIG_EMR_RUN`과 작업로그에 남습니다. Airflow에는 Amazon Provider와 등록한 `AWS_CONN_ID`가 필요합니다.

### 9.3 DAG 생성과 Airflow 배포

### 9.1 주제영역 전체·검증 DAG

| DAG ID | 역할 |
| --- | --- |
| `mig_{주제영역}_full_src_s3` | FULL 테이블 원천 → S3 Parquet, SRC·S3 검증 |
| `mig_{주제영역}_full_s3_tgt` | 검증 완료 S3 → 대상, S3·TGT 검증 |
| `mig_{주제영역}_full_all` | 원천 → S3 → SRC·S3 검증 → 대상 → S3·TGT 검증 |
| `mig_{주제영역}_vald_src_s3` | S3 기준본 재검증 |
| `mig_{주제영역}_vald_s3_tgt` | 대상 적재 재검증 |

### 9.2 테이블별 증분 DAG

| DAG ID | 역할 |
| --- | --- |
| `mig_{주제영역}_{매핑ID}_incr_src_s3` | 증분 원천 → S3, SRC·S3 검증 |
| `mig_{주제영역}_{매핑ID}_incr_s3_tgt` | S3 → 대상, S3·TGT 검증 |
| `mig_{주제영역}_{매핑ID}_incr_all` | S3 → SRC·S3 검증 → 대상 → S3·TGT 검증 |

### 9.3 일회성 재적재 DAG

`일회성 재적재` 생성은 기본 상태를 바꾸지 않습니다. `reload_src_s3`, `reload_s3_tgt`, `reload_all` 세 DAG가 만들어지며 전체 또는 WHERE 병렬 재적재에 사용합니다.

생성된 DAG 파일은 `10.Gp2Red/dag`에 남깁니다. 문제 확인을 위해 자동 삭제하지 않습니다. `DAG 저장·Airflow 배포`는 로컬 파일 저장, 지정 Airflow 배포, Airflow 인식 확인, paused 전환, 배포이력 기록을 한 번에 처리합니다.

| Airflow 항목 | 값 |
| --- | --- |
| Connection | 메타 DB: Variable `mig_metadata_conn_id`가 가리키는 Postgres/Redshift Connection |
| Connection | 원천·대상 DB: `TB_MIG_CONN.CONN_ID`와 동일한 이름 |
| Variable | `mig_metadata_conn_id` |
| Variable | `mig_executor_module` |
| Python 모듈 | `run_s3`, `run_s3_reset`, `run_s3_cleanup`, `run_ins`, `run_validate_src_s3`, `run_validate_s3_tgt` 실행 함수 |
| Airflow Provider | EMR 사용 시 Amazon Provider 및 `AWS_CONN_ID` |

생성 DAG는 이 여섯 실행 함수를 호출하는 공통 껍데기입니다. `run_s3_reset`은 FULL 추출 전에 테이블의 FULL 접두어를 한 번만 비우고, `run_s3`은 `record["src_extract_sql"]`을 실행해 원천 레이아웃 Parquet을 만들고 `s3_mnf_path`, `s3_data_path`를 반환합니다. SRC→S3 검증 성공 후 `run_s3_cleanup`은 최근 31일을 넘긴 INCR 접두어만 정리합니다. DAG는 실제 원천 조회조건과 함께 이를 `TB_MIG_S3_MANF`에 저장하고 반환값의 `s3_manf_id`를 TaskFlow XCom으로 다음 태스크에 전달합니다. `run_ins` 호출 전 대상 테이블명·대상 컬럼명·이행 SQL을 반영한 `DELETE·INSERT` SQL 계획을 `record["tgt_load_sql"]`에 담습니다. 실행 모듈은 매니페스트의 S3 데이터를 대상 스테이지에 준비한 뒤 `__MIG_STAGE__`를 실제 스테이지 테이블명으로 치환해 실행합니다. 실제 JDBC/ODBC/psycopg 추출·Parquet 생성·COPY·검증 SQL은 Airflow 실행 모듈에 구현합니다. Streamlit은 DAG 배포와 비활성 등록까지 수행하며, Airflow 실행 버튼을 대신 누르지는 않습니다.

### 실행 서버 설치 원칙

EC2 등 실행 서버에는 본체 소스를 동일하게 배포하고, 서버별 차이는 Airflow Connection·Variable·IAM 역할과 실행기 모듈에서만 관리합니다. `mig_executor_module` 변수로 서버 전용 실행기 모듈을 지정하므로, 원천 DB 드라이버·네트워크·S3 권한·파일 처리 방식이 달라도 `app`, `dag`, `sql` 본체를 수정할 필요가 없습니다. 실제 비밀번호와 접속문자열은 서버의 Airflow Connection 또는 `.streamlit/secrets.toml`에만 두고 Git에 넣지 않습니다.

증분 원천 S3 DAG는 아래처럼 시스템 기준값을 실행 설정으로 받습니다. 특정 부분을 직접 재작업할 때는 `src_where_cnd`를 넣으면 시스템 기준값 대신 그 조건을 그대로 사용합니다.

```json
{"sys_ref_val": "20260101000000"}
```

별도 `S3→대상` DAG를 실행할 때는 Airflow 실행 설정에 원천 S3 DAG의 실행 ID를 넣습니다.

```json
{"source_dag_run_id": "scheduled__2026-08-27T01:00:00+00:00"}
```

대상 DAG는 이 값과 대응하는 원천 S3 DAG명으로 `TB_MIG_S3_MANF`를 조회해 검증 성공·미반영 매니페스트만 묶어 처리합니다. 최신 매니페스트를 임의 조회하지 않으므로 재실행 시 다른 회차 S3 파일을 선택하지 않습니다.

## 10. 로그·검증·산출물

| 메타 테이블 | 책임 |
| --- | --- |
| `TB_MIG_CONN` | 접속 식별자, DBMS, 문자길이배수, S3 기준경로, Secrets 참조 |
| `TB_MIG_SBJ_AREA` | 주제영역 계층·원천·대상 접속정보·표시·사용여부 |
| `TB_MIG_SBJ_DAG_MPG` | 주제영역별 S3·대상·증분 병렬도와 일정 |
| `TB_MIG_SRC_LAYOUT` | 원천 구조 기준일 이력 |
| `TB_MIG_TBL_MPG`, `TB_MIG_COL_MPG` | 이행·변환·증분·검증 규칙과 대상 설명 |
| `TB_MIG_MPG_CHG_HIST` | 매핑 버전·변경이력 |
| `TB_MIG_S3_MANF` | 테이블·실행회차·병렬분할별 S3 Parquet 기준본, 실제 원천 조회조건, 검증·대상반영 상태 |
| `TB_MIG_DAG_RUN` | DAG별 전체·완료·진행·오류 건수와 진행률 |
| `TB_MIG_RUN_LOG` | 테이블별 S3·INS·검증 작업 로그 |
| `TB_MIG_VALD_RSLT`, `TB_MIG_VALD_COL_RSLT` | COUNT·SUM·HASH 검증 결과 |
| `TB_MIG_TBL_LOAD_HIST` | 기본 적재상태 전환 이력 |
| `TB_MIG_ARTF_ITEM` | Excel 산출물 항목·순서·출력여부 |

`실행 현황`은 5초마다 DAG별·테이블별 로그를 재조회해 전체/완료/진행중/오류 카드와 진행률을 표시합니다. 고객 제공 현황 화면이 필요하면 메타 DB에 읽기 전용 권한만 준 별도 Streamlit 배포본을 사용합니다.

## 10.1 대상 DDL과 설명

대상 DDL 화면은 테이블 매핑과 분리되어 있습니다. `대상 DDL 조회`를 누르면 등록된 대상 접속으로 `SHOW TABLE`을 실행해 분산·정렬·자동압축 설정을 읽습니다. 대상 테이블이 없거나 조회하지 않은 경우에는 `AUTO`, `AUTO`, 자동압축 해제로 시작합니다. 화면에서 분산·정렬·압축을 변경하면 `DROP TABLE IF EXISTS`, `CREATE TABLE`, 테이블·컬럼 `COMMENT ON` 문을 포함한 실행 DDL이 생성됩니다.

`대상 적용`은 실제 대상 접속에서 DDL을 실행합니다. `DROP TABLE 실행 확인`을 반드시 선택해야 하며, 대상 테이블 데이터는 삭제될 수 있습니다. 대상 테이블·컬럼 설명은 매핑에 저장하고, 한글과 줄바꿈은 UTF-8 및 표준 SQL 문자열로 보존합니다. Oracle 전용 `q'[]'` 문법은 Redshift에 사용하지 않습니다.

`산출물`은 테이블정의서, 컬럼정의서, 매핑정의서, 단위테스트결과서, 통합테스트결과서, 검증결과서를 각각 한 Excel 파일·한 시트로 생성합니다. 첫 행 고정, 굵게, 노란색 배경, 맑은 고딕 10포인트를 적용합니다.

## 11. 가상 검증과 운영 전 확인

```powershell
python 10.Gp2Red\tests\virtual_workflow_test.py -v
Get-ChildItem 10.Gp2Red\app -Filter *.py | ForEach-Object { python -m py_compile $_.FullName }
Get-ChildItem 10.Gp2Red\dag -Filter *.py -Recurse | ForEach-Object { python -m py_compile $_.FullName }
```

가상 검증은 DDL 계약, 타입 변환, FULL·INCR·검증 DAG 생성 문법, S3 실패 시 INS 미호출, 병렬 조건 제한, Excel 형식을 확인합니다. 실제 Redshift·S3·Airflow·원천 DB 접속은 포함하지 않습니다.

운영 전에는 Airflow에서 단일 테이블, 병렬도 1, S3·대상 검증 성공, 실패 후 재실행, 증분 범위 보정, 일회성 재적재를 각각 검증하십시오.

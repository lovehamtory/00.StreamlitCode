# SRC → S3 → TGT 이관 관리

Streamlit은 메타 관리, SQL·DAG 생성, Airflow 배포, 운영 조회와 산출물 생성을 담당합니다. 실제 추출·적재·검증 실행은 Airflow DAG가 담당합니다.

## 설치

Python 코드와 설치 파일은 분리해 배포합니다. `setup\\MigSetup.exe`를 더블클릭합니다.

`MigSetup.exe`는 Python 확인·설치, 가상환경 생성, 필수 라이브러리 설치, 실행 위치의 `app/.streamlit/secrets.toml` 생성을 처리합니다. 설치 중 창을 닫지 않습니다. 인터넷 설치에서 라이브러리 내려받기가 실패하면 서버에 복사한 `whl` 폴더를 선택해 오프라인 설치를 다시 시도합니다.

Python 설치 직후 설치가 중단되면 `MigSetup.exe`를 한 번 더 실행합니다.

### 인터넷 없는 서버 설치

인터넷 연결 PC에서 다음을 한 번 실행해 `whl` 모듈 폴더를 만듭니다.

```powershell
.\setup\download_offline_modules.ps1 -Destination D:\mig_wheels
```

`D:\mig_wheels` 폴더와 프로젝트 폴더를 서버로 복사한 뒤 `MigSetup.exe`를 실행합니다. 인터넷 설치가 실패하면 복사한 `whl` 폴더를 선택합니다. 설치 파일은 인터넷에 연결하지 않고 해당 폴더의 모듈만 설치합니다.

### 메타 DB 연결 설정

`app\.streamlit\secrets.toml` 파일을 열고 메타 DB 접속정보를 입력합니다. 실제 값은 개인 PC 또는 실행 서버에만 둡니다.

```toml
[migration_metadata]
connection_section = "tgt_red"

[src_gp]
host = ""
port = 5432
database = ""
user = ""
password = ""

[tgt_red]
host = ""
port = 5439
database = ""
user = ""
password = ""
```

## 실행·초기 설정

```powershell
.\setup\run.ps1
```

1. `admin/admin`으로 로그인합니다.
2. 메타가 없으면 `초기 설정`만 표시됩니다.
3. 메타 스키마명(예: `mig_meta`)을 입력하고 `메타 설치`를 실행합니다.
4. 다시 로그인하여 비밀번호를 변경합니다.

메타 설치는 스키마와 이관 메타 테이블·뷰를 만듭니다. 기존 이관 메타가 하나라도 있으면 설치를 중단하며, 업무 테이블과 원천·대상 물리 테이블은 변경하지 않습니다.

## 메뉴·업무 순서

| 순서 | 메뉴 | 주요 처리 |
| --- | --- | --- |
| 1 | 기준정보 > 접속정보 | DBMS·S3·Secrets 참조 등록 |
| 2 | 기준정보 > 주제영역 | 상위·하위 주제영역과 SRC·TGT 접속 지정 |
| 3 | 이관관리 > 테이블 레이아웃 | 원천 테이블·컬럼·PK·NULL 수집 |
| 4 | 이관관리 > 대상 DDL | 대상 DDL 조회·수정·적용 |
| 5 | 이관관리 > SRC·TGT 매핑 | 테이블·컬럼·이관 SQL·이행 SQL 등록 |
| 6 | 이관관리 > DAG 생성 | FULL·INCR·재적재 DAG 생성·배포 |
| 7 | 운영 > 실행 현황·검증·실행 이력 | DAG·테이블 상태와 검증 결과 확인 |
| 8 | 산출물 > 산출물 관리 | 정의서·매핑·테스트·검증 결과서 생성 |

`EMR`, `스냅샷 복구`, `테이블 변경 비교`는 운영 메뉴에서 필요할 때 사용합니다. `사용자 관리`, `권한 관리`, `초기 설정`은 설정 메뉴에 있습니다.

## 기준정보

### 접속정보

| 항목 | 값 |
| --- | --- |
| 접속 ID | 영문 시작, 영문·숫자·밑줄 |
| DBMS | `GREENPLUM`, `REDSHIFT`, `ORACLE`, `MSSQL`, `POSTGRESQL`, `OTHER` |
| 문자길이배수 | `1`, `2`, `3`, `4` |
| S3 기준경로 | `s3://bucket/prefix` 또는 공란 |
| Secrets 섹션명 | PC·서버의 secrets 섹션명 |
| 사용 | `Y`, `N` |

접속정보는 방향을 갖지 않습니다. SRC·TGT 방향은 주제영역에서 지정합니다. 실제 비밀번호와 접속문자열은 secrets 또는 Airflow Connection에만 둡니다.

### 주제영역

| 구분 | 예시 | 필수 항목 |
| --- | --- | --- |
| 상위주제영역 | `A01` | 상위주제영역코드, 상위주제영역명 |
| 주제영역 | `A010001` | 상위주제영역코드, 주제영역코드·명, SRC 접속 ID, TGT 접속 ID |

하위 주제영역이 DAG 분할 단위입니다. 테이블 매핑도 하위 주제영역에 연결합니다.

## 테이블 레이아웃·DDL

`테이블 레이아웃`에서 원천 기준일, 접속, 스키마를 선택해 레이아웃을 수집합니다. 수집 결과는 테이블·컬럼·PK·NULL 기준 이력입니다. `테이블 변경 비교`는 두 기준일의 차이를 확인합니다.

`대상 DDL`은 대상 테이블의 `SHOW TABLE` 결과를 읽고 DDL을 만듭니다. 분산·정렬·압축은 화면에서 수정할 수 있습니다. 적용 시 `DROP TABLE`, `CREATE TABLE`, `COMMENT ON`이 실행될 수 있으므로 적용 대상과 확인값을 검토합니다.

기본 타입 변환은 문자형 `VARCHAR(원천길이 × 문자길이배수)`, 숫자형 `DECIMAL`, 날짜·시간형 `TIMESTAMP`입니다. Redshift가 지원하는 동일 타입은 유지합니다.

## SRC·TGT 매핑

테이블 매핑은 대상 기준으로 관리합니다. 컬럼 매핑은 `MPG_ID`로 테이블 매핑에 연결됩니다.

| 항목 | 코드값 | 기준 |
| --- | --- | --- |
| 적재상태 | `FULL`, `INCR` | 기본 적재 대상 |
| 시스템컬럼 형식 | `YYYYMMDD`, `YYYYMMDDHH24MISS`, `TIMESTAMP`, `DATE` | 증분 기준값 형식 |
| 증분방식 | `PK_MERGE`, `APPEND` | 모두 대상 `DELETE` 후 `INSERT` |
| S3 병렬방식 | `NONE`, `WHERE` | WHERE 조건 배열별 SRC→S3 병렬 |
| 컬럼매핑방식 | `MOVE`, `CONST`, `NULL`, `EXPR` | 대상 컬럼 생성 규칙 |
| SUM·HASH 검증 | `Y`, `N` | 컬럼 단위 선택 |

- COUNT 검증은 항상 수행합니다.
- 시스템컬럼명·증분컬럼명·병렬조건은 쉼표·공백 또는 JSON 배열로 입력합니다.
- 신규 매핑의 증분 컬럼은 원천 PK를 기본값으로 사용합니다.

### SQL 탭

| 탭 | 용도 | 필수 치환값 |
| --- | --- | --- |
| S3 이관 SQL | SRC→S3 추출 SELECT | `__SRC_WHERE_CND__` |
| INS 이행 SQL | S3→TGT INSERT/COPY SELECT | `__MIG_STAGE__`, `__TGT_TABLE__` |

`SQL 생성`은 기본 SQL을 만들고, `SQL 저장`은 수정 SQL을 저장합니다. 저장 시 SELECT·INSERT 컬럼 수와 매핑 컬럼 수를 검증합니다. SQL 이력은 `TB_MIG_MPG_CHG_HIST`에 저장하며 복원할 수 있습니다.

S3 기준본 경로는 아래와 같습니다.

```text
full/{대상스키마}__{대상테이블}/
incr/{대상스키마}__{대상테이블}/wrk_dt=YYYYMMDD/run_id={DAG실행ID}/
```

FULL은 대상 경로를 재생성합니다. INCR은 최근 31일 기준본을 보관합니다. 대상 이행은 `TB_MIG_S3_MANF`에 기록된 검증 성공 매니페스트만 사용합니다.

## DAG 생성·배포

| 구분 | DAG ID |
| --- | --- |
| 주제영역 FULL S3 | `mig_{주제영역}_full_src_s3` |
| 주제영역 FULL INS | `mig_{주제영역}_full_s3_tgt` |
| 주제영역 FULL 통합 | `mig_{주제영역}_full_all` |
| 테이블 INCR S3 | `mig_{주제영역}_{매핑ID}_incr_src_s3` |
| 테이블 INCR INS | `mig_{주제영역}_{매핑ID}_incr_s3_tgt` |
| 테이블 INCR 통합 | `mig_{주제영역}_{매핑ID}_incr_all` |
| 일회성 재적재 | `reload_src_s3`, `reload_s3_tgt`, `reload_all` |

`DAG 생성`에서 Airflow, EMR, 기본·최대 병렬도, 일정, 대상 테이블을 지정합니다. 생성 시 DAG 파일을 보관하고 지정 Airflow에 paused 상태로 배포하며, 결과는 `TB_MIG_DAG_DPLY_HIST`에 기록합니다.

Airflow에는 아래 항목이 필요합니다.

| 구분 | 항목 |
| --- | --- |
| Connection | 메타 DB, 원천 DB, 대상 DB, AWS |
| Variable | `mig_metadata_conn_id`, `mig_executor_module` |
| 실행 모듈 | `run_s3`, `run_s3_reset`, `run_s3_cleanup`, `run_ins`, `run_validate_src_s3`, `run_validate_s3_tgt` |

## 운영

| 메뉴 | 확인 내용 |
| --- | --- |
| 실행 현황 | DAG·테이블별 전체, 완료, 진행중, 오류, 진행률 |
| 검증 | SRC·S3·TGT의 PK, COUNT, SUM, HASH |
| 실행 이력 | 작업일시, 경과초, 조건값, 오류 메시지 |
| EMR | 실행 이력, 전용 EMR 강제 종료 |
| 스냅샷 복구 | Redshift 스냅샷 복구 요청·결과 |

실행 현황은 5초 주기로 갱신합니다. 검증 실패 시 대상 반영 여부와 재실행 범위를 확인한 뒤 Airflow에서 재실행합니다.

## 산출물

선택한 시스템·DB·프로젝트와 사용 중인 매핑을 기준으로 아래 산출물을 생성합니다.

- 테이블정의서
- 컬럼정의서
- SRC·TGT 매핑정의서
- 단위테스트결과서
- 통합테스트결과서
- 검증결과서

각 산출물은 한 파일·한 시트로 생성하며 첫 행 고정, 굵게, 노란색 배경, 맑은 고딕 10포인트를 적용합니다.

## 사용자·권한

| 메뉴 | 관리 항목 |
| --- | --- |
| 권한 관리 | 권한그룹, 메뉴별 조회·저장 권한 |
| 사용자 관리 | 사용자, 권한그룹, 사용여부, 유효기간 |

신규 사용자의 초기 비밀번호는 사용자 ID와 같고, 첫 로그인에서 변경해야 합니다. 비밀번호는 해시로만 저장합니다.

## 검증·배포 전 확인

```powershell
python tests\virtual_workflow_test.py
Get-ChildItem app -Filter *.py | ForEach-Object { python -m py_compile $_.FullName }
```

가상 검증은 DDL 계약, 타입 변환, DAG 생성, 실패 차단, 병렬 조건, Excel 형식을 확인합니다. 실제 DB·S3·Airflow 접속 검증은 별도로 수행합니다.

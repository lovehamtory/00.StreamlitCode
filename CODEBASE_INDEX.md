# 코드베이스 인덱스

## 10.Gp2Red

SRC → S3 → TGT 이관을 위한 Streamlit 메타 관리·DAG 생성·Airflow 배포 도구입니다. 실제 데이터 실행은 Airflow가 담당합니다.

| 경로 | 역할 |
| --- | --- |
| `10.Gp2Red/app/SrcTgtOrchestrator.py` | 좌측 트리 메뉴·하단 작성자 표기가 있는 Streamlit 메뉴 진입점 |
| `10.Gp2Red/app/SrcTgtInitialize.py` | 클릭형 메타 초기 설정 화면 |
| `10.Gp2Red/app/SrcTgtControl.py` | 상위주제영역(A01)·주제영역(A010001) 계층, 하위 주제영역 접속정보, 매핑·DAG·검증·로그·산출물 통합 화면 |
| `10.Gp2Red/app/SrcTgtAirflow.py` | Airflow 환경, 공유경로·배포에이전트 방식 DAG 자동 배포, Airflow paused 등록과 배포이력 |
| `10.Gp2Red/app/SrcTgtEmr.py` | EMR 전용·공용 구분, 자동 종료 정책, 전용 EMR 수동 강제 종료 관리 화면 |
| `10.Gp2Red/app/SrcTgtLayoutHistory.py` | 원천 구조 수집·변경 비교·대상 반영안 화면 |
| `10.Gp2Red/app/SrcTgtDagGenerator.py` | 주제영역 FULL·검증·통합 DAG, 테이블 INCR·재적재 DAG, S3 FULL 초기화·INCR 31일 보관정리·매니페스트/XCom, Airflow 자동 배포, ALL DAG 전용 EMR 종료 |
| `10.Gp2Red/app/SrcTgtMapping.py` | 주제영역 접속정보를 상속하는 대상 기준 컬럼 매핑, MOVE·CONST·NULL·EXPR 규칙, 테이블 단위 이관·이행 SQL 생성·저장·이력복원·컬럼수 검증, PK 자동 기본값·시스템컬럼 증분 규칙 및 Excel 일괄 업로드 |
| `10.Gp2Red/app/SrcTgtMonitor.py` | 상위주제영역·주제영역별 DAG·테이블 실행 현황 화면 |
| `10.Gp2Red/app/SrcTgtArtifact.py` | Excel 산출물 및 레이아웃 정의 |
| `10.Gp2Red/app/SrcTgtSnapshotRestore.py` | Redshift 스냅샷 복구 |
| `10.Gp2Red/app/SrcTgtConnection.py` | 접속정보 관리 |
| `10.Gp2Red/app/SrcTgtDataType.py` | 원천 타입의 Redshift 표준 타입 변환 |
| `10.Gp2Red/app/SrcTgtLoadState.py` | FULL·INCR 상태, 쉼표·공백·JSON 컬럼 배열, 증분 방식, WHERE 병렬 입력 검증 |
| `10.Gp2Red/app/SrcTgtRuntime.py` | 메타 DB 공통 접속·식별자 함수 |
| `10.Gp2Red/app/SrcTgtSetup.py` | 메타 스키마 백업·DDL 실행 |
| `10.Gp2Red/app/SrcTgtTargetReflection.py` | 대상 DDL 조회·물리 옵션 편집·DROP/CREATE/COMMENT 적용 |
| `10.Gp2Red/app/SrcTgtValidation.py` | 검증 결과 조회 |
| `10.Gp2Red/dag/common/mig_step_runtime.py` | Airflow 실행기 공통 호출 함수 |
| `10.Gp2Red/dag/common/mig_emr_runtime.py` | Airflow AWS Connection을 사용하는 전용 EMR 종료 함수 |
| `10.Gp2Red/sql/01_mig_metadata_ddl.sql` | 접속·Airflow·EMR·DAG배포·실행·검증을 정의하는 Redshift 이관 메타 DDL |
| `10.Gp2Red/tests/virtual_workflow_test.py` | 문법·메타·DAG·실패차단 가상 검증 |
| `10.Gp2Red/README.md` | 운영자 매뉴얼 |

## 관리 원칙

- 실제 자격증명은 `.streamlit/secrets.toml`과 Airflow에만 둡니다.
- DAG 배포는 Airflow REST API가 아닌 공유 DAG 경로 또는 배포 에이전트로 수행한 뒤 Airflow API로 paused 상태를 등록합니다.
- EMR 자동 종료는 전용 EMR의 ALL DAG 종료 단계에서만 수행합니다.
- 생성 DAG는 `10.Gp2Red/dag`에 유지합니다.
- 상위주제영역은 분류·모니터링 단위이고 하위 주제영역은 접속정보·DAG 분할 단위이며 영역 간 오케스트레이션은 생성하지 않습니다.

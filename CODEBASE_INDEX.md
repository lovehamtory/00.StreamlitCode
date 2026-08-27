# 코드베이스 인덱스

## 10.Gp2Red

SRC → S3 → TGT 이관을 위한 Streamlit 메타 관리·DAG 생성 도구입니다. 실제 Airflow 배포와 실행은 포함하지 않습니다.

| 경로 | 역할 |
| --- | --- |
| `10.Gp2Red/app/SrcTgtOrchestrator.py` | Streamlit 메뉴 진입점 |
| `10.Gp2Red/app/SrcTgtInitialize.py` | 클릭형 메타 초기 설정 화면 |
| `10.Gp2Red/app/SrcTgtControl.py` | 접속·주제영역·매핑·DAG·검증·로그·산출물 통합 화면 |
| `10.Gp2Red/app/SrcTgtLayoutHistory.py` | 원천 구조 수집·변경 비교·대상 반영안 화면 |
| `10.Gp2Red/app/SrcTgtDagGenerator.py` | 주제영역 FULL·검증, 테이블 INCR·재적재 DAG 생성 |
| `10.Gp2Red/app/SrcTgtMapping.py` | 테이블·컬럼 매핑 및 Excel 일괄 업로드 |
| `10.Gp2Red/app/SrcTgtMonitor.py` | DAG·테이블 실행 현황 화면 |
| `10.Gp2Red/app/SrcTgtArtifact.py` | Excel 산출물 및 레이아웃 정의 |
| `10.Gp2Red/app/SrcTgtSnapshotRestore.py` | Redshift 스냅샷 복구 |
| `10.Gp2Red/app/SrcTgtConnection.py` | 접속정보 관리 |
| `10.Gp2Red/app/SrcTgtDataType.py` | 원천 타입의 Redshift 표준 타입 변환 |
| `10.Gp2Red/app/SrcTgtLoadState.py` | FULL·INCR 상태와 WHERE 병렬 입력 검증 |
| `10.Gp2Red/app/SrcTgtRuntime.py` | 메타 DB 공통 접속·식별자 함수 |
| `10.Gp2Red/app/SrcTgtSetup.py` | 메타 스키마 백업·DDL 실행 |
| `10.Gp2Red/app/SrcTgtTargetReflection.py` | 대상 물리 반영안·DDL 생성 |
| `10.Gp2Red/app/SrcTgtValidation.py` | 검증 결과 조회 |
| `10.Gp2Red/dag/common/mig_step_runtime.py` | Airflow 실행기 공통 호출 함수 |
| `10.Gp2Red/sql/01_mig_metadata_ddl.sql` | Redshift 이관 메타 DDL |
| `10.Gp2Red/tests/virtual_workflow_test.py` | 문법·메타·DAG·실패차단 가상 검증 |
| `10.Gp2Red/README.md` | 운영자 매뉴얼 |

## 관리 원칙

- 실제 자격증명은 `.streamlit/secrets.toml`과 Airflow에만 둡니다.
- 생성 DAG는 `10.Gp2Red/dag`에 유지합니다.
- 주제영역은 DAG 분할 단위이며 영역 간 오케스트레이션은 생성하지 않습니다.

from __future__ import annotations

import json
from typing import Any


LOAD_STATES = {"FULL", "INCR"}
SYSTEM_COLUMN_FORMATS = {"YYYYMMDD", "YYYYMMDDHH24MISS", "TIMESTAMP", "DATE"}
INCREMENT_METHODS = {"PK_MERGE", "APPEND"}
PARALLEL_METHODS = {"NONE", "WHERE"}


def text(value: object) -> str:
    return "" if value is None else str(value).strip()


def normalize_parallel(method: object, condition_array: object) -> dict[str, Any]:
    parallel_method = text(method).upper() or "NONE"
    if parallel_method not in PARALLEL_METHODS:
        raise ValueError("S3추출병렬방식코드는 NONE 또는 WHERE 중 하나여야 합니다.")
    if parallel_method == "NONE":
        return {"method": parallel_method, "count": 1, "conditions": []}
    try:
        raw = condition_array if isinstance(condition_array, list) else json.loads(text(condition_array) or "[]")
    except json.JSONDecodeError as error:
        raise ValueError("S3추출병렬조건배열은 JSON 배열이어야 합니다.") from error
    conditions = [text(value) for value in raw] if isinstance(raw, list) else []
    if not conditions or any(not value for value in conditions):
        raise ValueError("S3추출병렬조건배열은 비어 있지 않은 JSON 문자열 배열이어야 합니다.")
    forbidden = (";", "--", "/*", "*/")
    if any(any(token in value for token in forbidden) for value in conditions):
        raise ValueError("S3추출병렬조건에는 세미콜론 또는 SQL 주석을 입력할 수 없습니다.")
    return {"method": parallel_method, "count": len(conditions), "conditions": conditions}


def normalize_name_array(value: object, field_name: str, required: bool = False) -> list[str]:
    try:
        raw = value if isinstance(value, list) else json.loads(text(value) or "[]")
    except json.JSONDecodeError as error:
        raise ValueError(f"{field_name}은 JSON 문자열 배열이어야 합니다.") from error
    if not isinstance(raw, list) or any(not text(item) for item in raw):
        raise ValueError(f"{field_name}은 비어 있지 않은 JSON 문자열 배열이어야 합니다.")
    result = [text(item) for item in raw]
    if required and not result:
        raise ValueError(f"{field_name}은 필수입니다.")
    return result


def transition_plan(current_state: object, target_state: object, baseline_manifest_id: object, running: bool, system_columns: object, system_format: object, increment_method: object, increment_columns: object) -> dict[str, str | int | None]:
    current = text(current_state).upper()
    target = text(target_state).upper()
    if current not in LOAD_STATES or target not in LOAD_STATES:
        raise ValueError("적재상태를 확인하십시오.")
    if running:
        raise ValueError("실행 중인 테이블은 적재상태를 전환할 수 없습니다.")
    if current == target:
        raise ValueError("현재 적재방식과 동일한 방식으로는 전환할 수 없습니다.")
    if target == "INCR":
        normalize_name_array(system_columns, "시스템컬럼명", required=True)
        if text(system_format).upper() not in SYSTEM_COLUMN_FORMATS:
            raise ValueError("시스템컬럼 데이터 형식을 선택하십시오.")
        if text(increment_method).upper() not in INCREMENT_METHODS:
            raise ValueError("증분 방식을 선택하십시오.")
        normalize_name_array(increment_columns, "증분컬럼명", required=True)
    return {"before": current, "after": target, "runtime_method": target, "baseline_manifest_id": int(baseline_manifest_id) if baseline_manifest_id else None}

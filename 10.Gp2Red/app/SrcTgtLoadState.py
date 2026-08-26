from __future__ import annotations

import json
from typing import Any


LOAD_STATES = {"FULL", "INCR"}
INCR_BASIS_CODES = {"DT", "YMD", "YM", "WM_DTM", "PK"}
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


def transition_plan(current_state: object, target_state: object, baseline_manifest_id: object, running: bool, basis_code: object, basis_column: object) -> dict[str, str | int | None]:
    current = text(current_state).upper()
    target = text(target_state).upper()
    if current not in LOAD_STATES or target not in LOAD_STATES:
        raise ValueError("적재상태를 확인하십시오.")
    if running:
        raise ValueError("실행 중인 테이블은 적재상태를 전환할 수 없습니다.")
    if current == target:
        raise ValueError("현재 적재방식과 동일한 방식으로는 전환할 수 없습니다.")
    return {"before": current, "after": target, "runtime_method": target, "baseline_manifest_id": int(baseline_manifest_id) if baseline_manifest_id else None}


def recovery_window(last_success_value: object, requested_end_value: object, system_work_value: object) -> dict[str, str]:
    start = text(last_success_value)
    end = text(requested_end_value) or text(system_work_value)
    if not start:
        raise ValueError("증분 복구에는 마지막 검증 성공 기준값이 필요합니다.")
    if not end:
        raise ValueError("증분 복구 종료 기준값이 필요합니다.")
    if start >= end:
        raise ValueError("증분 복구 종료 기준값은 시작 기준값보다 커야 합니다.")
    return {"basis_start_value": start, "basis_end_value": end, "system_work_value": text(system_work_value)}

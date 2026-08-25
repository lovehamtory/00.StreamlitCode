from __future__ import annotations

import importlib
from datetime import datetime
from typing import Any, Callable


def execute_logged_step(record: dict[str, Any], step: str, label: str, executor_module: str, write_log: Callable[[dict[str, Any], str, str, str], None]) -> dict[str, Any]:
    record["task_name"] = step.lower()
    record["wrk_stt_dtm"] = datetime.now()
    write_log(record, step, "RUNNING", f"{label} 시작")
    try:
        if not executor_module:
            raise RuntimeError(f"{label} 실행기 모듈이 설정되지 않았습니다.")
        module = importlib.import_module(executor_module)
        handler = getattr(module, f"run_{step.lower()}", None)
        if not callable(handler):
            raise RuntimeError(f"{executor_module}.run_{step.lower()} 실행기를 찾을 수 없습니다.")
        result = handler(dict(record))
        if isinstance(result, dict):
            record.update(result)
    except Exception as error:
        write_log(record, step, "FAILED", str(error))
        raise
    write_log(record, step, "SUCCESS", f"{label} 완료")
    return record

from __future__ import annotations

import re


def text(value: object) -> str:
    return "" if value is None else str(value).strip()


def length_value(value: object) -> str:
    return re.sub(r"\s+", "", text(value))


def parameters(value: str, fallback: object = None) -> str:
    matched = re.search(r"\(([^)]+)\)", value)
    if matched:
        return re.sub(r"\s+", "", matched.group(1))
    return length_value(fallback)


def character_length_multiple(value: object = 1) -> int:
    try:
        multiple = int(value or 1)
    except (TypeError, ValueError) as error:
        raise ValueError("문자길이배수는 1~4 정수여야 합니다.") from error
    if multiple not in {1, 2, 3, 4}:
        raise ValueError("문자길이배수는 1, 2, 3, 4 중 하나여야 합니다.")
    return multiple


def varchar_length(value: str, fallback: object = None, multiple: object = 1) -> int:
    candidate = parameters(value, fallback)
    if not candidate:
        return 65535
    try:
        size = int(candidate.split(",")[0])
    except ValueError as error:
        raise ValueError(f"문자열 길이 형식이 올바르지 않습니다: {candidate}") from error
    target_size = size * character_length_multiple(multiple)
    if size < 1 or target_size > 65535:
        raise ValueError("VARCHAR 길이는 1~65535여야 합니다.")
    return target_size


def decimal_parameters(value: str, fallback: object = None) -> tuple[int, int]:
    candidate = parameters(value, fallback)
    if not candidate:
        return 38, 10
    pieces = candidate.split(",")
    try:
        precision = int(pieces[0])
        scale = int(pieces[1]) if len(pieces) > 1 else 0
    except ValueError as error:
        raise ValueError(f"DECIMAL 정밀도 형식이 올바르지 않습니다: {candidate}") from error
    if len(pieces) > 2 or precision < 1 or precision > 38 or scale < 0 or scale > precision:
        raise ValueError("DECIMAL 정밀도는 1~38, 소수점 자릿수는 0~정밀도여야 합니다.")
    return precision, scale


def redshift_type(source_type: object, source_length: object = None, character_multiple: object = 1) -> str:
    raw = re.sub(r"\s+", " ", text(source_type)).upper()
    base = re.sub(r"\s*\([^)]*\)", "", raw).strip()
    if base in {"CHAR", "CHARACTER", "BPCHAR", "VARCHAR", "CHARACTER VARYING", "NVARCHAR", "NCHAR", "NVARCHAR2", "TEXT", "CLOB"}:
        return f"VARCHAR({varchar_length(raw, source_length, character_multiple)})"
    if base in {"NUMERIC", "DECIMAL", "NUMBER", "MONEY", "SMALLMONEY"}:
        precision, scale = decimal_parameters(raw, source_length)
        return f"DECIMAL({precision},{scale})"
    if base in {"SMALLINT", "INT2"}:
        return "DECIMAL(38,0)"
    if base in {"INTEGER", "INT", "INT4", "SERIAL"}:
        return "DECIMAL(38,0)"
    if base in {"BIGINT", "INT8", "BIGSERIAL"}:
        return "DECIMAL(38,0)"
    if base in {"REAL", "FLOAT4"}:
        return "DECIMAL(38,10)"
    if base in {"FLOAT", "FLOAT8", "DOUBLE", "DOUBLE PRECISION"}:
        return "DECIMAL(38,10)"
    if base in {"DATE", "DATETIME", "DATETIME2", "SMALLDATETIME", "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"}:
        return "TIMESTAMP"
    if base in {"TIME", "TIME WITHOUT TIME ZONE", "TIME WITH TIME ZONE", "TIMETZ"}:
        return "TIME"
    if base in {"BOOLEAN", "BOOL", "BIT"}:
        return "BOOLEAN"
    if base in {"BYTEA", "BINARY", "VARBINARY", "BLOB", "RAW", "IMAGE"}:
        return "VARBYTE(65535)"
    if base in {"UUID", "UNIQUEIDENTIFIER"}:
        return "VARCHAR(36)"
    if base in {"JSON", "JSONB", "XML", "SQL_VARIANT", "GEOGRAPHY", "GEOMETRY"}:
        return "VARCHAR(65535)"
    raise ValueError(f"대상 Redshift 표준 데이터타입으로 변환할 수 없습니다: {source_type}")

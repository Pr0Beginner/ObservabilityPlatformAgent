"""Sensitive field policy shared by model inputs and diagnostic text."""

import json
import re
from typing import Any

SENSITIVE_NAMES = (
    "password",
    "passwd",
    "pwd",
    "authorization",
    "token",
    "cookie",
    "apikey",
    "secret",
    "credential",
)
KEY = (
    r"[\w.-]*(?:password|passwd|pwd|authorization|token|cookie|api[_-]?key|secret|credential)"
    r"[\w.-]*"
)
QUOTED_VALUE = r""""(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*' """.strip()
JSON_PAIR = re.compile(rf"""(["']{KEY}["']\s*:\s*)(?:{QUOTED_VALUE}|[^\s,;}}]+)""", re.I)
COOKIE = re.compile(r"(\bcookie\s*[:=]\s*)[^\r\n]+", re.I)
TEXT_PAIR = re.compile(rf"(\b{KEY}\s*[:=]\s*)(?:{QUOTED_VALUE}|(?:bearer\s+)?[^\s,;]+)", re.I)
PHONE = re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)")
REDACTED = "[REDACTED]"
MAX_DEPTH = 64


def is_sensitive(key: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", key.lower())
    return any(name in normalized for name in SENSITIVE_NAMES)


def redact_value(value: Any, depth: int = 0) -> Any:
    if depth >= MAX_DEPTH:
        return REDACTED
    if isinstance(value, dict):
        return {
            key: REDACTED if is_sensitive(str(key)) else redact_value(item, depth + 1)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_value(item, depth + 1) for item in value]
    if isinstance(value, str):
        return redact_sensitive(value, depth + 1)
    return value


def redact_sensitive(value: str, depth: int = 0) -> str:
    if depth >= MAX_DEPTH:
        return REDACTED
    try:
        parsed = json.loads(value)
    except (ValueError, RecursionError):
        parsed = None
    if isinstance(parsed, (dict, list)):
        return json.dumps(redact_value(parsed, depth + 1), ensure_ascii=False)
    result = JSON_PAIR.sub(lambda match: match[1] + '"[REDACTED]"', value)
    result = COOKIE.sub(lambda match: match[1] + REDACTED, result)
    result = TEXT_PAIR.sub(lambda match: match[1] + REDACTED, result)
    return PHONE.sub("1**********", result)

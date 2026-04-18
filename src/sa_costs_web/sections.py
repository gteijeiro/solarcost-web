from __future__ import annotations

import re
import unicodedata
from typing import Any

DEFAULT_SERVICE_SECTION_CODE = "service"
DEFAULT_TAX_SECTION_CODE = "tax"

SYSTEM_SECTIONS: tuple[dict[str, Any], ...] = (
    {
        "code": DEFAULT_SERVICE_SECTION_CODE,
        "name": "Servicio de energia",
        "position": 10,
        "is_system": True,
        "enabled": True,
    },
    {
        "code": DEFAULT_TAX_SECTION_CODE,
        "name": "IVA y otros conceptos",
        "position": 20,
        "is_system": True,
        "enabled": True,
    },
)


def is_system_section_code(code: str | None) -> bool:
    normalized = str(code or "").strip()
    return any(section["code"] == normalized for section in SYSTEM_SECTIONS)


def get_system_section(code: str | None) -> dict[str, Any] | None:
    normalized = str(code or "").strip()
    for section in SYSTEM_SECTIONS:
        if section["code"] == normalized:
            return dict(section)
    return None


def get_system_section_name(code: str | None) -> str | None:
    section = get_system_section(code)
    return str(section["name"]) if section is not None else None


def normalize_section_code(value: str) -> str:
    text = unicodedata.normalize("NFKD", value)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = " ".join(text.lower().strip().split())
    normalized = re.sub(r"[^a-z0-9_]+", "_", text.replace(" ", "_"))
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        return "section"
    if normalized[0].isdigit():
        return f"section_{normalized}"
    return normalized
